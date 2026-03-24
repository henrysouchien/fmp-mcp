"""Shared helpers for FMP MCP tools."""

from datetime import date, timedelta


def _last_trading_day() -> str:
    """Return the most recent weekday as YYYY-MM-DD (skips weekends)."""
    d = date.today()
    d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.isoformat()
