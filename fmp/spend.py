"""Default spend metering for hosts that do not inject their own FMP budget guard.

``fmp.client.configure_client()`` installs ``guard_call`` and ``record_cache_hit`` from
here when the host passes no ``budget_guard``, so every FMP network call in any process
writes one row to the machine-wide API spend ledger (``model_authority.spend_ledger``) and
every request answered from the disk cache writes a zero-call ``cache_hit`` row. A host that
passes its own ``budget_guard`` (Risk's ``guard_call``) owns metering and nothing here runs.

Row shape matches Risk's ``risk_app.api_budget.spend_rows`` so one ledger query covers both:
provider ``fmp`` (or ``fmp_estimates``), ``api_category`` ``market_data``,
``model_or_endpoint`` ``<provider>:<operation>``, credential fingerprint, ticker, HTTP status,
response bytes and duration. Attribution (repo, service) resolves through the ledger's own
mechanism (``configure_spend_process`` / ``HANK_SPEND_REPO``) because this package does not
know its host.

A spend ceiling already met raises ``SpendCeilingExceeded`` before any request leaves the
machine and writes a ``budget_blocked`` row. A ledger that cannot be read stops a PAYG
credential (``SpendLedgerUnavailable`` propagates) and is logged-and-allowed for a flat plan
key. Row writes never raise.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Mapping
from typing import Any

from model_authority.spend_ledger import (
    SpendLedgerUnavailable,
    classify_auth_kind,
    fingerprint_credential,
    record_api_spend,
)
from model_authority.spend_settings import SpendCeilingExceeded, enforce_spend_ceilings

logger = logging.getLogger(__name__)

# The query parameter that carries each guarded provider's API key on the request.
_CREDENTIAL_PARAM = {"fmp": "apikey", "fmp_estimates": "key"}


def _request_params(kwargs: Mapping[str, Any] | None) -> Mapping[str, Any]:
    params = (kwargs or {}).get("params")
    return params if isinstance(params, Mapping) else {}


def _ticker(params: Mapping[str, Any]) -> str | None:
    for name in ("symbol", "symbols"):
        value = params.get(name)
        if isinstance(value, str) and value.strip():
            return value.split(",")[0].strip().upper()
    return None


def _status_code(source: Any) -> int | None:
    for holder in (source, getattr(source, "response", None)):
        code = getattr(holder, "status_code", None)
        if isinstance(code, int) and not isinstance(code, bool):
            return code
    return None


def _failure_status(code: int | None) -> str:
    return "rate_limit" if code == 429 else "error"


def _response_bytes(result: Any) -> int | None:
    content = getattr(result, "content", None)
    return len(content) if isinstance(content, (bytes, bytearray)) else None


def guard_call(
    *,
    fn: Callable[..., Any],
    args: tuple = (),
    kwargs: Mapping[str, Any] | None = None,
    provider: str,
    operation: str,
    **_host_metadata: Any,
) -> Any:
    """Enforce the provider's ceilings, run ``fn`` and write one ledger row for the attempt."""
    auth_kind = classify_auth_kind(provider, "api_key")
    params = _request_params(kwargs)
    credential = params.get(_CREDENTIAL_PARAM.get(provider, ""))
    fingerprint = (
        fingerprint_credential(credential) if isinstance(credential, str) and credential else None
    )
    endpoint = f"{provider}:{operation}"
    ticker = _ticker(params)

    def write(**fields: Any) -> None:
        record_api_spend(
            api_category="market_data",
            provider=provider,
            model_or_endpoint=endpoint,
            auth_kind=auth_kind,
            credential_fingerprint=fingerprint,
            ticker=ticker,
            **fields,
        )

    try:
        enforce_spend_ceilings(
            provider=provider, auth_kind=auth_kind, credential_fingerprint=fingerprint
        )
    except SpendCeilingExceeded as exc:
        write(
            status="budget_blocked",
            error_type=type(exc).__name__,
            attrs={"breaches": [f"{b.ceiling}:{b.scope}" for b in exc.breaches]},
        )
        raise
    except SpendLedgerUnavailable as exc:
        if auth_kind == "payg_api_key":
            raise
        logger.warning("spend ledger unavailable for %s: %s", endpoint, exc)

    started = time.monotonic()
    try:
        result = fn(*args, **(kwargs or {}))
    except Exception as exc:
        code = _status_code(exc)
        write(
            duration_ms=(time.monotonic() - started) * 1000,
            http_status=code,
            status=_failure_status(code),
            error_type=type(exc).__name__,
        )
        raise
    code = _status_code(result)
    failed = code is not None and code >= 400
    write(
        duration_ms=(time.monotonic() - started) * 1000,
        http_status=code,
        response_bytes=_response_bytes(result),
        status=_failure_status(code) if failed else "ok",
    )
    return result


def record_cache_hit(endpoint: str, params: Mapping[str, Any] | None = None) -> None:
    """One zero-call row for a request answered from the disk cache, so hit rate is visible."""
    record_api_spend(
        api_category="market_data",
        provider="fmp",
        model_or_endpoint=f"fmp:{endpoint}",
        auth_kind=classify_auth_kind("fmp", "api_key"),
        ticker=_ticker(params or {}),
        api_calls=0,
        cache_hit=True,
        shadow_cost_usd=0.0,
    )


def observe(event: str, endpoint: str, **details: Any) -> None:
    """Event observer: only ``cache_hit`` is ledger-relevant (network calls are rows via ``guard_call``)."""
    if event == "cache_hit":
        record_cache_hit(endpoint, details.get("params"))
