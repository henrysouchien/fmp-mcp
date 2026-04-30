"""HTTP boundary for the hosted estimates service.

This endpoint is distinct from the main Financial Modeling Prep API and is
tracked separately as ``fmp_estimates`` for future cost-guard work.
"""

from __future__ import annotations

import os
from typing import Any

try:
    from app_platform.api_budget import guard_call
except ImportError:
    def guard_call(*, fn, args=(), kwargs=None, **_):
        """No-op fallback when app_platform.api_budget isn't installed (dist runtime)."""
        return fn(*args, **(kwargs or {}))

import requests as _requests

ESTIMATE_API_URL = os.getenv("ESTIMATE_API_URL", "https://financialmodelupdater.com")
ESTIMATE_API_KEY = os.getenv("EDGAR_API_KEY")
MISSING_API_URL_ERROR = (
    "ESTIMATE_API_URL environment variable is required. "
    "Set it to the hosted estimates API URL (e.g. https://financialmodelupdater.com)."
)


def get(
    path: str,
    params: dict[str, Any] | None = None,
    *,
    budget_user_id: int | None = None,
) -> list | dict:
    """Fetch JSON from the hosted estimates API."""
    request_params = dict(params or {})
    if ESTIMATE_API_KEY:
        request_params["key"] = ESTIMATE_API_KEY
    response = guard_call(
        provider="fmp_estimates",
        operation="get",
        budget_user_id=budget_user_id,
        cost_per_call=0,
        fn=_requests.get,
        args=(f"{ESTIMATE_API_URL}{path}",),
        kwargs={"params": request_params, "timeout": 15},
    )
    response.raise_for_status()
    return response.json()
