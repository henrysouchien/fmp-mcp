"""Fetch annual EDGAR concept values through the sibling edgar-parser API."""

from __future__ import annotations

import importlib
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


_TTL = timedelta(hours=24)
_CACHE: dict[tuple[str, str, int], tuple[datetime, tuple[float | None, dict, str]]] = {}


def fetch_concept(
    ticker: str,
    concept_name: str,
    fiscal_year: int,
) -> tuple[float | None, dict, str]:
    """Fetch one annual EDGAR concept value for a ticker/fiscal year."""

    normalized_ticker = ticker.strip().upper()
    normalized_concept = concept_name.strip()
    key = (normalized_ticker, normalized_concept, int(fiscal_year))
    now = datetime.now(UTC)
    cached = _CACHE.get(key)
    if cached and now - cached[0] < _TTL:
        return cached[1]

    retrieved_at = now.isoformat()
    get_metric = _load_get_metric()
    payload = get_metric(
        normalized_ticker,
        year=int(fiscal_year),
        quarter=None,
        metric_name=normalized_concept,
        full_year_mode=True,
        source="auto",
    )
    if not isinstance(payload, dict):
        payload = {"status": "error", "message": "edgar get_metric returned non-dict payload"}

    value = _first_match_value(payload)
    result = (value, payload, retrieved_at)
    _CACHE[key] = (now, result)
    return result


def _load_get_metric() -> Any:
    try:
        return importlib.import_module("edgar_parser.tools").get_metric
    except ModuleNotFoundError:
        sibling_root = Path(__file__).resolve().parents[3] / "edgar-parser"
        if sibling_root.exists():
            sys.path.insert(0, str(sibling_root))
        return importlib.import_module("edgar_parser.tools").get_metric


def _first_match_value(payload: dict[str, Any]) -> float | None:
    matches = payload.get("matches")
    if not isinstance(matches, list) or not matches:
        return None
    first_match = matches[0]
    if not isinstance(first_match, dict):
        return None
    return _parse_float(first_match.get("current_value"))


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    text = str(value).strip().replace(",", "")
    if text.endswith("%"):
        text = text[:-1].strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


__all__ = ["fetch_concept"]
