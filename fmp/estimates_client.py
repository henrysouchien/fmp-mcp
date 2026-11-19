"""HTTP boundary for the hosted estimates service.

This endpoint is distinct from the main Financial Modeling Prep API and is
tracked separately as ``fmp_estimates`` for future cost-guard work.
"""

from __future__ import annotations

import os
from typing import Any

from .client import guard_call
from .exceptions import FMPAPIError, FMPAuthenticationError, FMPRateLimitError

import requests as _requests

DEFAULT_ESTIMATE_API_URL = "https://www.edgarparser.com"
ESTIMATE_API_URL = os.getenv("ESTIMATE_API_URL", DEFAULT_ESTIMATE_API_URL)
ESTIMATE_API_KEY = os.getenv("EDGAR_API_KEY")
# The full-universe /api/estimates/revision-summary call measured 26.1s on
# 2026-07-16; a 15s timeout caused daily read timeouts starting 2026-07-08.
_REQUEST_TIMEOUT_SECONDS = 60
# What a caller is told when no estimates address is set. Only whoever runs the installation can set
# it, so the answer names no setting; the tools log the setting where they refuse.
ESTIMATES_NOT_CONFIGURED = (
    "Estimate revisions are not configured on this installation, so nothing was read. "
    "They are set up by whoever runs this installation."
)


def _dispatch(
    path: str,
    request_params: dict[str, Any],
    budget_user_id: int | None,
) -> _requests.Response:
    try:
        return guard_call(
            provider="fmp_estimates",
            operation="get",
            budget_user_id=budget_user_id,
            cost_per_call=0,
            fn=_requests.get,
            args=(f"{ESTIMATE_API_URL}{path}",),
            kwargs={"params": request_params, "timeout": _REQUEST_TIMEOUT_SECONDS},
        )
    except _requests.exceptions.RequestException as error:
        failure = type(error).__name__
    # Raise outside the handler so the credential-bearing Requests exception
    # is not retained as either an explicit cause or an implicit context.
    raise FMPAPIError(
        f"Hosted estimates request failed ({failure}) for '{path}'",
        endpoint=path,
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
    response = _dispatch(path, request_params, budget_user_id)

    if response.status_code == 401:
        error = FMPAuthenticationError(
            f"Hosted estimates authentication failed (HTTP 401) for '{path}'"
        )
        error.endpoint = path
        raise error
    if response.status_code == 429:
        raise FMPRateLimitError(endpoint=path)
    if not response.ok:
        raise FMPAPIError(
            f"Hosted estimates HTTP {response.status_code} for '{path}'",
            status_code=response.status_code,
            endpoint=path,
        )
    try:
        payload = response.json()
    except _requests.exceptions.JSONDecodeError:
        raise FMPAPIError("Invalid JSON response from hosted estimates", endpoint=path) from None
    if not isinstance(payload, (list, dict)):
        raise FMPAPIError("Hosted estimates response must be a list or object", endpoint=path)
    return payload
