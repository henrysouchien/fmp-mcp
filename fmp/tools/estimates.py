"""MCP tools for analyst estimate revision tracking."""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from typing import Literal, Optional, Any

try:
    from ..estimate_store import EstimateStore
except ImportError:
    EstimateStore = None


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
        if EstimateStore is None:
            return {
                "status": "error",
                "error": "Estimate tools require optional dependency 'psycopg2-binary'. Install fmp-mcp[estimates].",
            }

        clean_ticker = str(ticker).strip().upper()
        if not clean_ticker:
            return {
                "status": "error",
                "error": "ticker is required",
            }

        with EstimateStore(read_only=True) as store:
            latest = store.get_latest(clean_ticker, period=period)
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

            revisions = store.get_revisions(
                clean_ticker,
                fiscal_date=resolved_fiscal,
                period=period,
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
        if EstimateStore is None:
            return {
                "status": "error",
                "error": "Estimate tools require optional dependency 'psycopg2-binary'. Install fmp-mcp[estimates].",
            }

        if days < 0:
            return {
                "status": "error",
                "error": "days must be non-negative",
            }

        clean_tickers = _normalize_tickers(tickers)
        with EstimateStore(read_only=True) as store:
            target_tickers = clean_tickers or None
            summary = store.get_revision_summary(
                tickers=target_tickers,
                days=days,
                period=period,
            )

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
