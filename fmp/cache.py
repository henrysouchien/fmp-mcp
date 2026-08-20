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
import json
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Iterable, Literal

import pandas as pd
from fmp._shared.timeseries_store import (
    TimeSeriesStore,
    _atomic_write_parquet,
    _atomic_write_text,
    _is_expired,
    _safe_load,
)

SERVICE_CACHE_MAXSIZE = int(os.getenv("FMP_CACHE_MAXSIZE", "200"))
_CACHE_LINEAGE_SCHEMA_VERSION = "fmp-cache-lineage.v1"


@dataclass(frozen=True, slots=True)
class FMPCacheReadResult:
    """A cache delivery with its original provider observation time."""

    dataframe: pd.DataFrame
    source: Literal["live", "cache"]
    observed_at: datetime


def _lineage_path(path: Path) -> Path:
    return path.with_suffix(f"{path.suffix}.lineage.json")


def _delete_cache_entry(path: Path) -> None:
    path.unlink(missing_ok=True)
    _lineage_path(path).unlink(missing_ok=True)


def _aware_utc(value: datetime) -> datetime:
    offset = value.utcoffset() if value.tzinfo is not None else None
    if offset is None:
        raise ValueError("cache observation clock must return an aware datetime")
    return value.astimezone(timezone.utc)


def _valid_sha256(value: object) -> bool:
    if not isinstance(value, str) or len(value) != 64:
        return False
    return all(character in "0123456789abcdef" for character in value)


def _read_lineage_metadata(path: Path) -> dict[str, Any] | None:
    metadata_path = _lineage_path(path)
    if not metadata_path.is_file():
        return None
    try:
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return None
        if payload.get("schema_version") != _CACHE_LINEAGE_SCHEMA_VERSION:
            return None
        observed_at = datetime.fromisoformat(
            str(payload["observed_at"]).replace("Z", "+00:00")
        )
        observed_at = _aware_utc(observed_at)
        request_sha256 = payload["request_sha256"]
        response_sha256 = payload["response_sha256"]
        if not _valid_sha256(request_sha256) or not _valid_sha256(
            response_sha256
        ):
            return None
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
        return None
    return {
        "schema_version": _CACHE_LINEAGE_SCHEMA_VERSION,
        "observed_at": observed_at,
        "request_sha256": request_sha256,
        "response_sha256": response_sha256,
    }


def _write_lineage_metadata(
    path: Path,
    *,
    observed_at: datetime,
    request_sha256: str,
    response_sha256: str,
) -> None:
    payload = json.dumps(
        {
            "schema_version": _CACHE_LINEAGE_SCHEMA_VERSION,
            "observed_at": _aware_utc(observed_at).isoformat(),
            "request_sha256": request_sha256,
            "response_sha256": response_sha256,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    _atomic_write_text(payload, _lineage_path(path))


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
                _delete_cache_entry(path)
            else:
                df = _safe_load(path)
                if df is not None:
                    return df
                _lineage_path(path).unlink(missing_ok=True)

        # Cache miss - compute and store
        df = loader()
        if not df.empty:
            _atomic_write_parquet(df, path)
            _lineage_path(path).unlink(missing_ok=True)
        return df

    def read_with_metadata(
        self,
        *,
        key: Iterable[str | int | float | None],
        loader: Callable[[], pd.DataFrame],
        request_sha256: str,
        response_sha256_for: Callable[[pd.DataFrame], str],
        clock: Callable[[], datetime],
        cache_dir: str = "cache/fmp",
        prefix: str | None = None,
        ttl_hours: int | None = None,
    ) -> FMPCacheReadResult:
        """Read a cache entry only when its observation metadata is valid."""

        if not _valid_sha256(request_sha256):
            raise ValueError("request_sha256 must be a lowercase SHA-256 digest")
        path = self._get_cache_path(cache_dir, key, prefix)

        if path.is_file():
            if _is_expired(path, ttl_hours):
                _delete_cache_entry(path)
            else:
                dataframe = _safe_load(path)
                if dataframe is None:
                    _lineage_path(path).unlink(missing_ok=True)
                else:
                    metadata = _read_lineage_metadata(path)
                    response_sha256 = response_sha256_for(dataframe)
                    if (
                        metadata is not None
                        and metadata["request_sha256"] == request_sha256
                        and metadata["response_sha256"] == response_sha256
                    ):
                        return FMPCacheReadResult(
                            dataframe=dataframe,
                            source="cache",
                            observed_at=metadata["observed_at"],
                        )
                    _delete_cache_entry(path)
        else:
            _lineage_path(path).unlink(missing_ok=True)

        dataframe = loader()
        observed_at = _aware_utc(clock())
        response_sha256 = response_sha256_for(dataframe)
        if not _valid_sha256(response_sha256):
            raise ValueError("response_sha256_for returned an invalid digest")
        if not dataframe.empty:
            _atomic_write_parquet(dataframe, path)
            _write_lineage_metadata(
                path,
                observed_at=observed_at,
                request_sha256=request_sha256,
                response_sha256=response_sha256,
            )
        return FMPCacheReadResult(
            dataframe=dataframe,
            source="live",
            observed_at=observed_at,
        )

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
        _lineage_path(path).unlink(missing_ok=True)
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
            _delete_cache_entry(path)
            return True
        _lineage_path(path).unlink(missing_ok=True)
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
