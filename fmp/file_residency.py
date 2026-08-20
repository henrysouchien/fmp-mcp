"""Cheap local-file residency checks that never open file content."""

from __future__ import annotations

from pathlib import Path
import stat


def is_dataless_file(path: str | Path) -> bool:
    """Return whether *path* is a macOS File Provider placeholder.

    ``SF_DATALESS`` files have metadata and a logical size but no resident
    content. Opening one can block indefinitely while File Provider attempts
    to materialize it. Platforms without ``SF_DATALESS`` always return
    ``False``.
    """
    dataless_flag = int(getattr(stat, 'SF_DATALESS', 0))
    if dataless_flag == 0:
        return False
    flags = int(getattr(Path(path).stat(), 'st_flags', 0))
    return bool(flags & dataless_flag)


__all__ = ['is_dataless_file']
