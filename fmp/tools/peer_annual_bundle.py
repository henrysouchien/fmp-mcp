"""Fetch annual FMP endpoint rows for one peer and fiscal year."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from fmp.tools.fmp_core import fmp_fetch


_TTL = timedelta(hours=24)
_ANNUAL_ENDPOINTS = (
    "income_statement",
    "balance_sheet",
    "cash_flow",
    "key_metrics",
    "ratios",
)
_CACHE: dict[tuple[str, int], tuple[datetime, tuple[dict[str, Any], dict[str, str]]]] = {}


def fetch_peer_annual_bundle(
    ticker: str,
    fiscal_year: int,
) -> tuple[dict[str, Any], dict[str, str]]:
    """Fetch and filter annual FMP endpoint rows for a ticker/fiscal year."""

    normalized_ticker = ticker.strip().upper()
    normalized_year = int(fiscal_year)
    key = (normalized_ticker, normalized_year)
    now = datetime.now(UTC)
    cached = _CACHE.get(key)
    if cached and now - cached[0] < _TTL:
        return cached[1]

    retrieved_at_by_endpoint: dict[str, str] = {}
    bundle: dict[str, Any] = {}
    for endpoint in _ANNUAL_ENDPOINTS:
        retrieved_at = datetime.now(UTC).isoformat()
        retrieved_at_by_endpoint[endpoint] = retrieved_at
        response = fmp_fetch(
            endpoint,
            symbol=normalized_ticker,
            period="annual",
            limit=12,
        )
        bundle[endpoint] = _matching_fiscal_year_row(response, normalized_year)

    result = (bundle, retrieved_at_by_endpoint)
    _CACHE[key] = (now, result)
    return result


def _matching_fiscal_year_row(response: Any, fiscal_year: int) -> dict[str, Any] | None:
    if not isinstance(response, dict) or response.get("status") == "error":
        return None
    rows = response.get("data")
    if isinstance(rows, dict):
        rows = [rows]
    if not isinstance(rows, list):
        return None
    for row in rows:
        if isinstance(row, dict) and _row_matches_fiscal_year(row, fiscal_year):
            return row
    return None


def _row_matches_fiscal_year(row: dict[str, Any], fiscal_year: int) -> bool:
    for key in ("fiscalYear", "calendarYear", "year"):
        value = row.get(key)
        if value is not None and _parse_year(value) == fiscal_year:
            return True
    date_value = row.get("date")
    if date_value is not None and _parse_year(str(date_value)[:4]) == fiscal_year:
        return True
    return False


def _parse_year(value: Any) -> int | None:
    try:
        return int(str(value)[:4])
    except (TypeError, ValueError):
        return None


__all__ = ["fetch_peer_annual_bundle"]
