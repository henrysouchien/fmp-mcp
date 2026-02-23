"""Shared file-output utilities for FMP MCP tools."""

from __future__ import annotations

import csv
import os
import tempfile
from pathlib import Path



def _cache_base() -> Path:
    """Resolve the base cache directory."""
    env = os.getenv("FMP_CACHE_DIR")
    if env:
        return Path(env).expanduser().resolve()

    project_root = Path(__file__).parent.parent.parent
    if (project_root / "settings.py").exists():
        return project_root

    xdg = os.getenv("XDG_CACHE_HOME", os.path.expanduser("~/.cache"))
    return Path(xdg) / "fmp-mcp"


FILE_OUTPUT_DIR = _cache_base() / "cache" / "file_output"


def atomic_write_text(path: Path, content: str) -> None:
    """Write content to path atomically (tempfile + rename)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            delete=False,
        ) as tmp:
            tmp.write(content)
            tmp_path = Path(tmp.name)
        os.replace(tmp_path, path)
    finally:
        if tmp_path is not None and tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass


def _collect_fieldnames(records: list[dict]) -> list[str]:
    """Collect CSV fieldnames from records while preserving first-seen order."""
    fieldnames: list[str] = []
    seen: set[str] = set()
    for record in records:
        for key in record.keys():
            key_str = str(key)
            if key_str not in seen:
                seen.add(key_str)
                fieldnames.append(key_str)
    return fieldnames


def write_csv(records: list[dict], path: Path) -> None:
    """Write list of dict records to CSV atomically."""
    if not records:
        atomic_write_text(path, "")
        return

    fieldnames = _collect_fieldnames(records)
    path.parent.mkdir(parents=True, exist_ok=True)

    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            delete=False,
            newline="",
        ) as tmp:
            writer = csv.DictWriter(tmp, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for record in records:
                row = {name: record.get(name) for name in fieldnames}
                writer.writerow(row)
            tmp_path = Path(tmp.name)
        os.replace(tmp_path, path)
    finally:
        if tmp_path is not None and tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass


def _is_numeric(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def auto_summary(records: list[dict], date_col: str = "date") -> dict:
    """Generate summary stats for a list of record dicts."""
    summary: dict = {"row_count": len(records)}
    if not records:
        return summary

    dates = [str(r.get(date_col)) for r in records if r.get(date_col) is not None]
    if dates:
        summary["date_range"] = {"earliest": min(dates), "latest": max(dates)}

    column_order = _collect_fieldnames(records)
    numeric_cols = [
        col
        for col in column_order
        if col != date_col and any(_is_numeric(r.get(col)) for r in records)
    ]

    column_stats: dict[str, dict] = {}
    for col in numeric_cols:
        values = [float(r[col]) for r in records if _is_numeric(r.get(col))]
        if not values:
            continue

        stats: dict[str, float] = {
            "min": min(values),
            "max": max(values),
            "latest": values[0],
        }
        if len(values) >= 2 and values[-1] != 0:
            stats["pct_change"] = round((values[0] - values[-1]) / abs(values[-1]) * 100, 2)

        column_stats[col] = stats

    if column_stats:
        summary["column_stats"] = column_stats

    return summary
