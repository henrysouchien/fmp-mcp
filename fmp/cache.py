"""FMP Caching Layer.

Disk-based caching with Parquet + Zstandard compression for persistence.
Reuses patterns from data_loader.py for consistency.

Note: LRU cache utilities (_lru_fetch, clear_lru_cache, lru_cache_info) are
provided for future use but not currently wired into FMPClient.fetch().

Agent orientation:
    ``FMPCache.read`` is the canonical disk-cache contract used by
    ``fmp.client.FMPClient``.
"""

from __future__ import annotations

import hashlib
import os
import threading
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Iterable

import pandas as pd
from utils.timeseries_store import (
    TimeSeriesStore,
    _MISSING_COVERAGE_MTIME,
    _atomic_write_parquet,
    _atomic_write_text,
    _coerce_date_bound,
    _coerce_month_end_bound,
    _date_str,
    _is_empty_loader_error,
    _is_expired,
    _merge_series,
    _normalize_series,
    _safe_load,
)

SERVICE_CACHE_MAXSIZE = int(os.getenv("FMP_CACHE_MAXSIZE", "200"))


def _hash(parts: Iterable[str | int | float | None]) -> str:
    """Generate a short deterministic hash from key parts."""
    key = "_".join(str(p) for p in parts if p is not None)
    return hashlib.md5(key.encode()).hexdigest()[:8]


class FMPCache:
    """Two-tier cache manager for FMP data."""

    def __init__(self, base_dir: str | Path = "."):
        self.base_dir = Path(base_dir).expanduser().resolve()

    def _get_cache_path(
        self,
        cache_dir: str,
        key: Iterable[str | int | float | None],
        prefix: str | None = None,
    ) -> Path:
        """Build the cache file path."""
        dir_path = self.base_dir / cache_dir
        dir_path.mkdir(parents=True, exist_ok=True)
        key_list = list(key)
        fname = f"{prefix or key_list[0]}_{_hash(key_list)}.parquet"
        return dir_path / fname

    def read(
        self,
        *,
        key: Iterable[str | int | float | None],
        loader: Callable[[], pd.DataFrame],
        cache_dir: str = "cache/fmp",
        prefix: str | None = None,
        ttl_hours: int | None = None,
    ) -> pd.DataFrame:
        """
        Read from cache or compute via loader on miss.

        Contract notes:
        - Returns cached DataFrame when present and fresh.
        - Calls ``loader`` only on miss/expiry/corruption.
        - Writes non-empty loader results back to disk.

        Args:
            key: Cache key components
            loader: Function to call on cache miss
            cache_dir: Cache directory name
            prefix: Filename prefix
            ttl_hours: Time-to-live in hours (None = no expiry)

        Returns:
            DataFrame from cache or loader
        """
        path = self._get_cache_path(cache_dir, key, prefix)

        if path.is_file():
            if _is_expired(path, ttl_hours):
                path.unlink(missing_ok=True)
            else:
                df = _safe_load(path)
                if df is not None:
                    return df

        # Cache miss - compute and store
        df = loader()
        if not df.empty:
            _atomic_write_parquet(df, path)
        return df

    def write(
        self,
        df: pd.DataFrame,
        *,
        key: Iterable[str | int | float | None],
        cache_dir: str = "cache/fmp",
        prefix: str | None = None,
    ) -> Path:
        """Force-write a DataFrame to cache."""
        path = self._get_cache_path(cache_dir, key, prefix)
        _atomic_write_parquet(df, path)
        return path

    def invalidate(
        self,
        *,
        key: Iterable[str | int | float | None],
        cache_dir: str = "cache/fmp",
        prefix: str | None = None,
    ) -> bool:
        """Remove a specific cache entry."""
        path = self._get_cache_path(cache_dir, key, prefix)
        if path.is_file():
            path.unlink()
            return True
        return False


# Module-level cache instance (uses project root)
_cache: FMPCache | None = None
_timeseries_stores: dict[str, TimeSeriesStore] = {}
_timeseries_store_guard = threading.Lock()


def _default_cache_base() -> Path:
    env = os.getenv("FMP_CACHE_DIR")
    if env:
        return Path(env).expanduser().resolve()
    project_root = Path(__file__).parent.parent
    if (project_root / "settings.py").exists():
        return project_root
    xdg = os.getenv("XDG_CACHE_HOME", os.path.expanduser("~/.cache"))
    return Path(xdg) / "fmp-mcp"


def get_cache() -> FMPCache:
    """Get or create the module-level cache instance."""
    global _cache
    if _cache is None:
        _cache = FMPCache(_default_cache_base())
    return _cache


def get_timeseries_store(base_dir: str | Path | None = None) -> TimeSeriesStore:
    """Get or create the per-base-dir time series store singleton."""
    resolved = Path(base_dir or _default_cache_base()).expanduser().resolve()
    key = str(resolved)
    with _timeseries_store_guard:
        store = _timeseries_stores.get(key)
        if store is None:
            store = TimeSeriesStore(resolved)
            _timeseries_stores[key] = store
        return store


def _clear_all_timeseries_stores(series_kind: str | None = None) -> None:
    """Clear cached files across all instantiated time series stores."""
    with _timeseries_store_guard:
        stores = list(_timeseries_stores.values())
    for store in stores:
        store.clear(series_kind=series_kind)


def _reset_timeseries_store_registry_for_tests() -> None:
    """Drop store singletons for test isolation."""
    with _timeseries_store_guard:
        _timeseries_stores.clear()


# LRU cache utilities (available for future use, not currently wired into FMPClient)
@lru_cache(maxsize=SERVICE_CACHE_MAXSIZE)
def _lru_fetch(
    endpoint_name: str,
    cache_key_tuple: tuple,
) -> tuple[tuple, tuple]:
    """
    LRU wrapper that returns tuple representation of DataFrame.

    NOTE: This function is provided for future LRU caching integration but is
    not currently called by FMPClient.fetch(). Disk caching is the primary
    caching mechanism.
    """
    # This function would be called with already-fetched data converted to tuple
    # It serves as an LRU pass-through for hot data
    return cache_key_tuple


def clear_lru_cache() -> None:
    """Clear the LRU cache."""
    _lru_fetch.cache_clear()


def lru_cache_info() -> Any:
    """Get LRU cache statistics."""
    return _lru_fetch.cache_info()
