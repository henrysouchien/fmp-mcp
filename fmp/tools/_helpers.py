"""Shared helpers for FMP MCP tools."""

from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Any


# Vendored from portfolio_math._utils so the fmp package stays importable
# outside the monorepo (the standalone fmp-mcp wheel has no utils/portfolio_math).
def safe_float(value: Any, default: float | None = 0.0) -> float | None:
    """Best-effort finite float coercion with a caller-selected fallback."""

    if value is None or isinstance(value, bool):
        return default
    if isinstance(value, str):
        value = value.strip().replace("%", "").replace(",", "")
        if not value:
            return default
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(numeric):
        return default
    return numeric


def safe_float_or_none(value: Any) -> float | None:
    """Best-effort finite float coercion with ``None`` fallback."""

    return safe_float(value, default=None)


def _last_trading_day() -> str:
    """Return the most recent weekday as YYYY-MM-DD (skips weekends)."""
    d = date.today()
    d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.isoformat()


__all__ = ["_last_trading_day", "safe_float", "safe_float_or_none"]
