"""
MCP Tools: FMP (Financial Modeling Prep) API

Exposes FMP data fetching as MCP tools for AI invocation.
Wraps the existing fmp.client module with structured responses.

Usage (from Claude):
    "What FMP endpoints are available?" -> fmp_list_endpoints()
    "How do I use the income_statement endpoint?" -> fmp_describe("income_statement")
    "Get Apple's income statement" -> fmp_fetch("income_statement", symbol="AAPL")
    "Search for semiconductor companies" -> fmp_search("semiconductor")
    "Get Apple's profile" -> fmp_profile("AAPL")

Architecture:
- All functions return structured dicts with status/data/error fields
- Exceptions are caught and mapped to error responses (never thrown)
- Delegates all fetching to FMPClient (single source of truth)
"""

import math
import re
from datetime import datetime
from typing import Any, Literal, Optional

from ..client import get_client
from ..exceptions import (
    FMPAPIError,
    FMPAuthenticationError,
    FMPEmptyResponseError,
    FMPEndpointError,
    FMPRateLimitError,
    FMPValidationError,
)
from ..registry import get_categories
from ._file_output import FILE_OUTPUT_DIR, auto_summary, write_csv


def _error_response(
    error_type: str,
    message: str,
    endpoint: Optional[str] = None,
    params: Optional[dict] = None,
) -> dict:
    """Build a structured error response."""
    response = {
        "status": "error",
        "error_type": error_type,
        "message": message,
    }
    if endpoint:
        response["endpoint"] = endpoint
    if params:
        response["params"] = params
    return response


def _map_exception_to_error(
    e: Exception,
    endpoint: Optional[str] = None,
    params: Optional[dict] = None,
) -> dict:
    """Map FMP exceptions to structured error responses."""
    if isinstance(e, FMPRateLimitError):
        return _error_response("rate_limit", str(e), endpoint, params)
    elif isinstance(e, FMPAuthenticationError):
        return _error_response("auth", str(e), endpoint, params)
    elif isinstance(e, FMPValidationError):
        return _error_response("validation", str(e), endpoint, params)
    elif isinstance(e, FMPEmptyResponseError):
        return _error_response("empty_data", str(e), endpoint, params)
    elif isinstance(e, FMPEndpointError):
        return _error_response("unknown_endpoint", str(e), endpoint, params)
    elif isinstance(e, FMPAPIError):
        return _error_response("api", str(e), endpoint, params)
    else:
        return _error_response("unknown", str(e), endpoint, params)


def fmp_fetch(
    endpoint: str,
    symbol: Optional[str] = None,
    period: Optional[str] = None,
    limit: Optional[int] = None,
    columns: Optional[list[str]] = None,
    output: Literal["inline", "file"] = "inline",
    use_cache: bool = True,
    **kwargs: Any,
) -> dict:
    """
    Fetch data from any registered FMP endpoint.

    For financial-statement-style endpoints such as income_statement,
    balance_sheet, cash_flow, and key_metrics, the `date` field on each row is
    the period-end / as-of date for that row's metrics. For key_metrics,
    `marketCap` is aligned to that row's period end; use it for historical
    period analysis rather than current valuation. key_metrics_ttm rows reflect
    trailing-twelve-month metrics ending at FMP's server-side cutoff; that cutoff
    is not surfaced as a period-end column in the row.

    Args:
        endpoint: Name of the FMP endpoint (e.g., "income_statement", "historical_price_adjusted")
        symbol: Stock symbol (required for most endpoints)
        period: Reporting period ("annual" or "quarter") for financial statements
        limit: Maximum number of records to return
        columns: Optional list of columns to keep in the response
        output: "inline" (default) or "file" to write results to CSV
        use_cache: Whether to use cached data (default: True)
        **kwargs: Additional endpoint-specific parameters (e.g., from_date, to_date)

    Returns:
        dict with:
            - status: "success" or "error"
            - endpoint: The endpoint name
            - params: Parameters used
            - row_count: Number of records returned (success only)
            - columns: List of column names (success only)
            - data: List of record dicts (success only, inline output)
            - file_path: CSV path (success only, file output)
            - error_type, message: Error details (error only)

    Examples:
        fmp_fetch("income_statement", symbol="AAPL", period="annual", limit=3)
        fmp_fetch("historical_price_adjusted", symbol="AAPL", from_date="2024-01-01")
        fmp_fetch("treasury_rates", from_date="2024-01-01", to_date="2024-12-31")
    """
    # Build params dict, filtering out None values
    params = {}
    if symbol:
        params["symbol"] = symbol
    if period:
        params["period"] = period
    if limit:
        params["limit"] = limit
    params.update({k: v for k, v in kwargs.items() if v is not None})

    # Resolve effective limit: explicit > endpoint default
    effective_limit = limit
    if not effective_limit:
        from ..registry import get_endpoint
        ep = get_endpoint(endpoint)
        if ep:
            for p in ep.params:
                if p.name == "limit" and p.default is not None:
                    effective_limit = p.default
                    break

    try:
        client = get_client()
        df = client.fetch(endpoint, use_cache=use_cache, **params)

        # Enforce limit client-side (some APIs ignore the limit param)
        if effective_limit and len(df) > effective_limit:
            df = df.head(effective_limit)

        filtered_columns: list[str] = []
        warnings: list[str] = []
        if columns:
            requested_cols = [c for c in columns if isinstance(c, str)]
            filtered_columns = [c for c in requested_cols if c in df.columns]
            if filtered_columns:
                df = df[filtered_columns]
            else:
                warnings.append(
                    "None of the requested columns were found. Returning all available columns."
                )

        # Convert DataFrame to records
        records = df.to_dict(orient="records")

        response: dict[str, Any] = {
            "status": "success",
            "endpoint": endpoint,
            "params": params,
            "row_count": len(records),
            "columns": list(df.columns),
        }
        if columns:
            response["filtered_columns"] = filtered_columns
        if warnings:
            response["warnings"] = warnings
        if len(records) > 50:
            response["summary"] = auto_summary(records)

        if output == "file":
            endpoint_slug = re.sub(r"[^A-Za-z0-9._-]+", "_", endpoint).strip("_") or "endpoint"
            symbol_slug = re.sub(r"[^A-Za-z0-9._-]+", "_", (symbol or "no_symbol")).strip("_")
            timestamp = datetime.now().strftime("%Y%m%dT%H%M%S%f")
            file_path = FILE_OUTPUT_DIR / f"{endpoint_slug}_{symbol_slug}_{timestamp}.csv"
            write_csv(records, file_path)
            response["output"] = "file"
            response["file_path"] = str(file_path)
            response["hint"] = "Use Read tool with file_path, or Grep to search columns."
            return response

        response["output"] = "inline"
        response["data"] = records
        return response

    except Exception as e:
        return _map_exception_to_error(e, endpoint, params)


def fmp_search(query: str, limit: int = 10, exchange: Optional[str] = None) -> dict:
    """
    Search for companies by name or ticker.

    Args:
        query: Search query (company name or partial ticker)
        limit: Maximum number of results (default: 10)
        exchange: Filter by exchange (e.g., "NASDAQ", "NYSE")

    Returns:
        dict with:
            - status: "success" or "error"
            - query: The search query
            - result_count: Number of matches
            - results: List of matching companies with symbol, name, exchange, etc.

    Examples:
        fmp_search("apple")
        fmp_search("semiconductor", limit=20)
        fmp_search("tech", exchange="NASDAQ")
    """
    params = {"query": query, "limit": limit}
    if exchange:
        params["exchange"] = exchange

    try:
        client = get_client()
        df = client.fetch("search", use_cache=False, **params)
        results = df.to_dict(orient="records")

        return {
            "status": "success",
            "query": query,
            "result_count": len(results),
            "results": results,
        }

    except Exception as e:
        return _map_exception_to_error(e, "search", params)


def fmp_profile(symbol: str) -> dict:
    """
    Get detailed company profile as a current FMP /profile snapshot.

    The profile `mktCap` field is price times shares as of the API call time,
    or up to 1 week stale when served from cache. FMP does not include a
    snapshot timestamp on this endpoint. For historical period-aligned market
    cap, use fmp_fetch(endpoint="key_metrics", period="annual") and read the
    `date` field on each row.

    Args:
        symbol: Stock symbol (e.g., "AAPL", "MSFT")

    Returns:
        dict with:
            - status: "success" or "error"
            - symbol: The requested symbol
            - profile: Company profile data (name, sector, industry, description, etc.)

    Examples:
        fmp_profile("AAPL")
        fmp_profile("MSFT")
    """
    try:
        client = get_client()
        df = client.fetch("profile", symbol=symbol)
        records = df.to_dict(orient="records")

        # Profile returns a list with single item
        profile = records[0] if records else {}

        return {
            "status": "success",
            "symbol": symbol,
            "profile": profile,
        }

    except Exception as e:
        return _map_exception_to_error(e, "profile", {"symbol": symbol})


def _positive_float(value: Any) -> Optional[float]:
    """Return a positive finite float for numeric-like values, otherwise None."""
    if value is None or isinstance(value, bool):
        return None
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric_value) or numeric_value <= 0:
        return None
    return numeric_value


def _extract_error_context(result: dict, source: str) -> dict:
    """Normalize an underlying tool error for fmp_market_cap_check."""
    error_type = result.get("error_type") or f"{source}_error"
    message = result.get("message") or f"{source} lookup failed"
    return _error_response(
        error_type,
        f"{source} lookup failed: {message}",
        result.get("endpoint") or source,
        result.get("params"),
    )


def fmp_market_cap_check(symbol: str) -> dict:
    """
    Compare current profile market cap against latest annual key_metrics market cap.

    Calls fmp_profile(symbol) for the current /profile snapshot and
    fmp_fetch(endpoint="key_metrics", symbol=symbol, period="annual", limit=1)
    for the latest annual period-aligned market cap. The profile `mktCap` value
    is for current valuation; the key_metrics `marketCap` value is aligned to
    `latest_annual_date` for historical period analysis.

    Args:
        symbol: Stock symbol (e.g., "AAPL", "MSFT")

    Returns:
        dict with status, symbol, profile_mktcap, latest_annual_mktcap,
        latest_annual_date, delta_pct, and warning. On error, returns
        status="error" with error_type and message.
    """
    try:
        profile_result = fmp_profile(symbol=symbol)
        if profile_result.get("status") == "error":
            return _extract_error_context(profile_result, "profile")

        key_metrics_result = fmp_fetch(
            endpoint="key_metrics",
            symbol=symbol,
            period="annual",
            limit=1,
        )
        if key_metrics_result.get("status") == "error":
            return _extract_error_context(key_metrics_result, "key_metrics")

        profile = profile_result.get("profile")
        if not isinstance(profile, dict):
            profile = {}

        rows = key_metrics_result.get("data")
        latest_row = rows[0] if isinstance(rows, list) and rows and isinstance(rows[0], dict) else {}

        profile_mktcap = _positive_float(profile.get("mktCap"))
        latest_annual_mktcap = _positive_float(latest_row.get("marketCap"))
        latest_annual_date = latest_row.get("date")

        response: dict[str, Any] = {
            "status": "success",
            "symbol": symbol,
            "profile_mktcap": profile_mktcap,
            "latest_annual_mktcap": latest_annual_mktcap,
            "latest_annual_date": latest_annual_date,
            "delta_pct": None,
            "warning": None,
        }

        missing_values = []
        if profile_mktcap is None:
            missing_values.append("profile_mktcap")
        if latest_annual_mktcap is None:
            missing_values.append("latest_annual_mktcap")

        if missing_values:
            verb = "is" if len(missing_values) == 1 else "are"
            response["warning"] = (
                "Cannot compute market-cap delta because "
                + " and ".join(missing_values)
                + f" {verb} missing, non-numeric, or zero/non-positive."
            )
            return response

        delta_pct = abs(profile_mktcap - latest_annual_mktcap) / max(
            profile_mktcap,
            latest_annual_mktcap,
        )
        response["delta_pct"] = delta_pct

        if delta_pct > 0.20:
            response["warning"] = (
                f"Profile market cap and latest annual key_metrics market cap differ by "
                f"{delta_pct:.1%}. Use profile_mktcap for current valuation; use "
                f"latest_annual_mktcap (as of {latest_annual_date}) for historical "
                "period analysis."
            )

        return response

    except Exception as e:
        return _map_exception_to_error(e, "fmp_market_cap_check", {"symbol": symbol})


def fmp_list_endpoints(category: Optional[str] = None) -> dict:
    """
    List available FMP endpoints.

    This is a discovery tool - use it to see what data is available
    before calling fmp_fetch.

    Args:
        category: Filter by category (e.g., "fundamentals", "prices", "analyst").
                  If not provided, returns all endpoints.

    Returns:
        dict with:
            - status: "success"
            - categories: List of available categories
            - endpoint_count: Number of endpoints
            - endpoints: List of endpoint summaries with name, category, description

    Examples:
        fmp_list_endpoints()
        fmp_list_endpoints(category="fundamentals")
    """
    try:
        client = get_client()
        endpoints = client.list_endpoints(category)
        categories = get_categories()

        return {
            "status": "success",
            "categories": categories,
            "endpoint_count": len(endpoints),
            "endpoints": endpoints,
            "filter_category": category,
        }

    except Exception as e:
        return _map_exception_to_error(e)


def fmp_describe(endpoint: str) -> dict:
    """
    Get detailed documentation for an FMP endpoint.

    Use this to understand what parameters an endpoint accepts
    before calling fmp_fetch.

    Args:
        endpoint: Name of the endpoint (e.g., "income_statement")

    Returns:
        dict with:
            - status: "success" or "error"
            - endpoint: The endpoint name
            - documentation: Full endpoint details including:
                - name, path, description, category
                - fmp_docs_url: Link to official FMP documentation
                - cache_dir, cache_ttl_hours, cache_enabled
                - parameters: List of parameter definitions with
                  name, type, required, default, description, enum_values

    Examples:
        fmp_describe("income_statement")
        fmp_describe("historical_price_adjusted")
    """
    try:
        client = get_client()
        docs = client.describe(endpoint)

        if docs is None:
            return _error_response(
                "unknown_endpoint",
                f"Unknown FMP endpoint: '{endpoint}'",
                endpoint,
            )

        return {
            "status": "success",
            "endpoint": endpoint,
            "documentation": docs,
        }

    except Exception as e:
        return _map_exception_to_error(e, endpoint)


# MCP Tool registration metadata (for reference, not used by FastMCP)
TOOL_METADATA = {
    "fmp_fetch": {
        "name": "fmp_fetch",
        "description": "Fetch data from any FMP endpoint. Use fmp_list_endpoints to discover available endpoints.",
    },
    "fmp_search": {
        "name": "fmp_search",
        "description": "Search for companies by name or ticker.",
    },
    "fmp_profile": {
        "name": "fmp_profile",
        "description": "Get current company profile snapshot information.",
    },
    "fmp_market_cap_check": {
        "name": "fmp_market_cap_check",
        "description": "Compare current profile market cap against latest annual key_metrics market cap.",
    },
    "fmp_list_endpoints": {
        "name": "fmp_list_endpoints",
        "description": "List available FMP endpoints. Discovery tool for fmp_fetch.",
    },
    "fmp_describe": {
        "name": "fmp_describe",
        "description": "Get documentation for an FMP endpoint. Discovery tool for fmp_fetch.",
    },
}
