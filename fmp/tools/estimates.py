"""MCP tools for analyst estimate revision tracking via hosted HTTP API.

This CLIENT-SIDE module runs locally as part of the fmp-mcp server and calls
the deployed estimates API (EC2 FastAPI -> RDS). ESTIMATE_API_URL must be set.

Server-side code lives in the edgar_updater repo:
  - edgar_updater/estimates/store.py
  - edgar_updater/estimates/collector.py
  - edgar_updater/edgar_api/routes/estimates.py
"""

from __future__ import annotations

import os
import sys
from datetime import date, datetime, timezone
from typing import Literal, Optional, Any

import requests as _requests

_ESTIMATE_API_URL = os.getenv("ESTIMATE_API_URL", "https://financialmodelupdater.com")
_ESTIMATE_API_KEY = os.getenv("EDGAR_API_KEY")
_MISSING_API_URL_ERROR = (
    "ESTIMATE_API_URL environment variable is required. "
    "Set it to the hosted estimates API URL (e.g. https://financialmodelupdater.com)."
)


def _api_get(path: str, params: dict | None = None) -> list | dict:
    """Fetch from the hosted estimates API."""
    params = dict(params or {})
    if _ESTIMATE_API_KEY:
        params["key"] = _ESTIMATE_API_KEY
    resp = _requests.get(f"{_ESTIMATE_API_URL}{path}", params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _normalize_tickers(tickers: Optional[list[str] | str]) -> list[str]:
    if tickers is None:
        return []

    if isinstance(tickers, str):
        raw = tickers.split(",")
    else:
        raw = tickers

    return sorted({str(t).strip().upper() for t in raw if str(t).strip()})


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def _select_default_fiscal_date(latest_rows: list[dict[str, Any]]) -> str | None:
    if not latest_rows:
        return None

    today = _utc_today()
    today_iso = today.isoformat()

    def _distance_days(value: Any) -> int:
        try:
            return abs((date.fromisoformat(str(value)[:10]) - today).days)
        except ValueError:
            return 99999

    ordered = sorted(
        latest_rows,
        key=lambda row: (
            0 if str(row.get("fiscal_date", "")) >= today_iso else 1,
            _distance_days(row.get("fiscal_date")),
            str(row.get("fiscal_date", "")),
        ),
    )
    return str(ordered[0].get("fiscal_date"))[:10] if ordered else None


def _delta(current: float | None, baseline: float | None) -> float | None:
    if current is None or baseline is None:
        return None
    return current - baseline


def _direction(eps_delta: float | None, revenue_delta: float | None) -> str:
    signal = eps_delta if eps_delta is not None else revenue_delta
    if signal is None:
        return "unknown"
    if signal > 0:
        return "up"
    if signal < 0:
        return "down"
    return "flat"


def get_estimate_revisions(
    ticker: str,
    fiscal_date: Optional[str] = None,
    period: Literal["quarter", "annual"] = "quarter",
) -> dict:
    """Get revision history for a ticker and fiscal period."""
    _saved = sys.stdout
    sys.stdout = sys.stderr
    try:
        clean_ticker = str(ticker).strip().upper()
        if not clean_ticker:
            return {
                "status": "error",
                "error": "ticker is required",
            }

        if not _ESTIMATE_API_URL:
            return {
                "status": "error",
                "error": _MISSING_API_URL_ERROR,
            }

        return _get_estimate_revisions_http(clean_ticker, fiscal_date, period)

    except Exception as exc:  # noqa: BLE001 - MCP tool should always return structured errors
        return {
            "status": "error",
            "error": str(exc),
            "ticker": str(ticker).strip().upper(),
            "fiscal_date": fiscal_date,
            "period": period,
        }
    finally:
        sys.stdout = _saved


def _get_estimate_revisions_http(
    clean_ticker: str,
    fiscal_date: str | None,
    period: str,
) -> dict:
    """Get estimate revisions via hosted HTTP API."""
    latest = _api_get("/api/estimates/latest", {"ticker": clean_ticker, "period": period})
    if not latest:
        return {
            "status": "success",
            "ticker": clean_ticker,
            "period": period,
            "fiscal_date": fiscal_date,
            "revision_count": 0,
            "revisions": [],
            "note": "No estimate snapshots found for ticker.",
        }

    resolved_fiscal = str(fiscal_date)[:10] if fiscal_date else _select_default_fiscal_date(latest)
    if not resolved_fiscal:
        return {
            "status": "success",
            "ticker": clean_ticker,
            "period": period,
            "fiscal_date": fiscal_date,
            "revision_count": 0,
            "revisions": [],
            "note": "Unable to determine fiscal_date from latest snapshots.",
        }

    revisions = _api_get(
        "/api/estimates/revisions",
        {"ticker": clean_ticker, "fiscal_date": resolved_fiscal, "period": period},
    )

    if not revisions:
        return {
            "status": "success",
            "ticker": clean_ticker,
            "period": period,
            "fiscal_date": resolved_fiscal,
            "revision_count": 0,
            "revisions": [],
            "note": "No snapshots found for requested fiscal period.",
        }

    first = revisions[0]
    last = revisions[-1]
    eps_delta = _delta(last.get("eps_avg"), first.get("eps_avg"))
    revenue_delta = _delta(last.get("revenue_avg"), first.get("revenue_avg"))

    return {
        "status": "success",
        "ticker": clean_ticker,
        "period": period,
        "fiscal_date": resolved_fiscal,
        "revision_count": len(revisions),
        "first_snapshot_date": first.get("snapshot_date"),
        "latest_snapshot_date": last.get("snapshot_date"),
        "eps_delta": eps_delta,
        "revenue_delta": revenue_delta,
        "direction": _direction(eps_delta, revenue_delta),
        "revisions": revisions,
    }


def screen_estimate_revisions(
    tickers: Optional[list[str] | str] = None,
    days: int = 30,
    direction: Literal["up", "down", "all"] = "all",
    period: Literal["quarter", "annual"] = "quarter",
) -> dict:
    """Screen a ticker universe for estimate momentum over a lookback window."""
    _saved = sys.stdout
    sys.stdout = sys.stderr
    try:
        if days < 0:
            return {
                "status": "error",
                "error": "days must be non-negative",
            }

        if not _ESTIMATE_API_URL:
            return {
                "status": "error",
                "error": _MISSING_API_URL_ERROR,
            }

        clean_tickers = _normalize_tickers(tickers)

        summary = _screen_http(clean_tickers, days, period)

        if direction != "all":
            summary = [item for item in summary if item.get("direction") == direction]

        summary.sort(
            key=lambda row: abs(
                row.get("eps_delta")
                if row.get("eps_delta") is not None
                else (row.get("revenue_delta") or 0.0)
            ),
            reverse=True,
        )

        return {
            "status": "success",
            "period": period,
            "days": days,
            "direction": direction,
            "tickers_requested": clean_tickers if clean_tickers else "all",
            "result_count": len(summary),
            "results": summary,
        }

    except Exception as exc:  # noqa: BLE001 - MCP tool should always return structured errors
        return {
            "status": "error",
            "error": str(exc),
            "period": period,
            "days": days,
            "direction": direction,
        }
    finally:
        sys.stdout = _saved


def _screen_http(
    clean_tickers: list[str],
    days: int,
    period: str,
) -> list[dict]:
    """Get revision summary via hosted HTTP API."""
    params: dict[str, Any] = {"days": days, "period": period}
    if clean_tickers:
        params["tickers"] = ",".join(clean_tickers)
    return _api_get("/api/estimates/revision-summary", params)
