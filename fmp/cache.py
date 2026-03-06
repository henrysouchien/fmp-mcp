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
import tempfile
import time
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Iterable

import pandas as pd
from pandas.errors import EmptyDataError, ParserError

SERVICE_CACHE_MAXSIZE = int(os.getenv("FMP_CACHE_MAXSIZE", "200"))


def _hash(parts: Iterable[str | int | float | None]) -> str:
    """Generate a short deterministic hash from key parts."""
    key = "_".join(str(p) for p in parts if p is not None)
    return hashlib.md5(key.encode()).hexdigest()[:8]


def _safe_load(path: Path) -> pd.DataFrame | None:
    """Safely load a parquet file, removing corrupted files."""
    try:
        return pd.read_parquet(path)
    except (EmptyDataError, ParserError, OSError, ValueError) as e:
        print(f"Cache file corrupted, deleting: {path.name} ({type(e).__name__})")
        path.unlink(missing_ok=True)
        return None


def _is_expired(path: Path, ttl_hours: int | None) -> bool:
    """Check if a cached file has expired based on TTL."""
    if ttl_hours is None:
        return False
    age_hours = (time.time() - path.stat().st_mtime) / 3600
    return age_hours > ttl_hours


def _atomic_write_parquet(df: pd.DataFrame, path: Path) -> None:
    """Write parquet atomically via temporary file + replace."""
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        df.to_parquet(tmp_path, engine="pyarrow", compression="zstd", index=True)
        os.replace(tmp_path, path)
    finally:
        tmp_path.unlink(missing_ok=True)


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
