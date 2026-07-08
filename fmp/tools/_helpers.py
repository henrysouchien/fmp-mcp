"""Shared helpers for FMP MCP tools."""

from datetime import date, timedelta

from utils.numeric import safe_float, safe_float_or_none


def _last_trading_day() -> str:
    """Return the most recent weekday as YYYY-MM-DD (skips weekends)."""
    d = date.today()
    d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.isoformat()


__all__ = ["_last_trading_day", "safe_float", "safe_float_or_none"]
