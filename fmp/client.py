"""FMP Client.

Single entry point for all FMP data fetching with:
- Unified fetch interface
- Automatic disk caching (Parquet + Zstandard compression)
- Endpoint discovery and documentation
- Error handling with structured logging

Agent orientation:
    Canonical runtime boundary for all registered FMP endpoint calls.
    Routing/config of endpoints lives in ``fmp.registry``; this module owns
    parameter validation, request execution, and cache policy application.
"""

from __future__ import annotations

import csv
import io
import os
import threading
import time
from collections import deque
from functools import lru_cache
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv

from .cache import FMPCache, get_cache
from .exceptions import (
    FMPAPIError,
    FMPAuthenticationError,
    FMPEmptyResponseError,
    FMPEndpointError,
    FMPRateLimitError,
    FMPValidationError,
)
from .registry import (
    CacheRefresh,
    FMPEndpoint,
    get_categories,
    get_endpoint,
    list_endpoints as registry_list_endpoints,
)

# Load environment
load_dotenv()


def _get_month_token() -> str:
    """Get current month token for cache key freshness (YYYYMM format)."""
    return pd.Timestamp.now().strftime("%Y%m")


def _extract_by_path(data: dict, path: str) -> Any:
    """
    Extract nested data using dot-path notation.

    Args:
        data: Dictionary to extract from
        path: Dot-separated path (e.g., "historical", "data.items", "response.data.records")

    Returns:
        Extracted data, or None if path not found
    """
    result = data
    for key in path.split("."):
        if isinstance(result, dict) and key in result:
            result = result[key]
        else:
            return None
    return result


class _RateLimiter:
    """Simple sliding-window rate limiter."""

    def __init__(self, max_calls_per_minute: int = 700) -> None:
        if max_calls_per_minute <= 0:
            raise ValueError("max_calls_per_minute must be positive")
        self._max = max_calls_per_minute
        self._timestamps: deque[float] = deque()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Block until a request slot is available."""
        while True:
            sleep_for = 0.0
            with self._lock:
                now = time.monotonic()
                while self._timestamps and self._timestamps[0] <= now - 60:
                    self._timestamps.popleft()

                if len(self._timestamps) < self._max:
                    self._timestamps.append(now)
                    return

                sleep_until = self._timestamps[0] + 60
                sleep_for = sleep_until - now

            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                time.sleep(0.001)


class FMPClient:
    """
    Unified client for FMP API access.

    Called by:
    - ``fmp.compat`` wrappers for backward compatibility
    - Direct callers using ``from fmp import fetch, get_client``

    Example usage:
        fmp = FMPClient()

        # Fetch data
        prices = fmp.fetch("historical_price_adjusted", symbol="AAPL", **{"from": "2020-01-01"})
        income = fmp.fetch("income_statement", symbol="AAPL", period="quarter")

        # Discover endpoints (works without API key)
        fmp.list_endpoints()                    # All endpoints
        fmp.list_endpoints(category="analyst")  # Filter by category
        fmp.describe("income_statement")        # Full documentation
    """

    # Base URLs for different API versions
    BASE_URL_STABLE = "https://financialmodelingprep.com/stable"
    BASE_URL_V3 = "https://financialmodelingprep.com/api/v3"

    def __init__(
        self,
        api_key: str | None = None,
        cache: FMPCache | None = None,
        timeout: int = 30,
        max_calls_per_minute: int = 700,
    ):
        """
        Initialize FMP client.

        API key validation is lazy - only checked when fetch() is called.
        This allows list_endpoints() and describe() to work without credentials.

        Args:
            api_key: FMP API key (defaults to FMP_API_KEY env var)
            cache: Custom cache instance (defaults to module cache)
            timeout: Request timeout in seconds
            max_calls_per_minute: Sliding-window request cap for all FMP calls
        """
        # Lazy API key - store but don't validate yet
        self._api_key = api_key
        self._api_key_resolved = False
        self.cache = cache or get_cache()
        self.timeout = timeout
        self._rate_limiter = _RateLimiter(max_calls_per_minute)

    @property
    def api_key(self) -> str:
        """Get API key, resolving from environment if needed."""
        if not self._api_key_resolved:
            if self._api_key is None:
                self._api_key = os.getenv("FMP_API_KEY")
            self._api_key_resolved = True
        return self._api_key or ""

    def _ensure_api_key(self) -> None:
        """Ensure API key is available, raise if not."""
        if not self.api_key:
            raise FMPAuthenticationError("FMP_API_KEY not found in environment")

    BASE_URL_V4 = "https://financialmodelingprep.com/api/v4"

    def _build_url(self, endpoint: FMPEndpoint, params: dict[str, Any]) -> str:
        """Build the full API URL for an endpoint."""
        # Use explicit api_version if set, otherwise infer from path
        if endpoint.api_version == "v3":
            base = self.BASE_URL_V3
            path = endpoint.path.replace("/api/v3", "").lstrip("/")
        elif endpoint.api_version == "v4":
            base = self.BASE_URL_V4
            path = endpoint.path.replace("/api/v4", "").lstrip("/")
        else:
            base = self.BASE_URL_STABLE
            path = endpoint.path.replace("/stable", "").lstrip("/")

        # Handle path parameters (e.g., /profile/{symbol})
        if "{symbol}" in path and "symbol" in params:
            path = path.replace("{symbol}", params["symbol"])

        return f"{base}/{path}"

    _RATE_LIMIT_RETRIES = 3
    _RATE_LIMIT_BACKOFF_SECONDS = 30

    def _make_request(
        self,
        endpoint: FMPEndpoint,
        params: dict[str, Any],
    ) -> list | dict:
        """Make HTTP request to FMP API with error handling."""
        self._ensure_api_key()
        self._rate_limiter.acquire()

        url = self._build_url(endpoint, params)

        # Add API key to params
        request_params = {"apikey": self.api_key}

        # Add other params, excluding path params
        for key, value in params.items():
            if f"{{{key}}}" not in endpoint.path:
                request_params[key] = value

        for attempt in range(1, self._RATE_LIMIT_RETRIES + 1):
            start_time = time.time()

            try:
                resp = requests.get(url, params=request_params, timeout=self.timeout)
            except requests.exceptions.Timeout:
                self._log_error(endpoint.name, "Request timeout")
                raise FMPAPIError(
                    f"Request timeout for endpoint '{endpoint.name}'",
                    endpoint=endpoint.name,
                )
            except requests.exceptions.RequestException as e:
                self._log_error(endpoint.name, str(e))
                raise FMPAPIError(
                    f"Request failed for endpoint '{endpoint.name}': {e}",
                    endpoint=endpoint.name,
                )

            response_time = time.time() - start_time

            # Handle rate limiting with retry
            if resp.status_code == 429:
                if attempt < self._RATE_LIMIT_RETRIES:
                    self._log_rate_limit(endpoint.name)
                    time.sleep(self._RATE_LIMIT_BACKOFF_SECONDS)
                    self._rate_limiter.acquire()
                    continue
                self._log_rate_limit(endpoint.name)
                raise FMPRateLimitError(endpoint=endpoint.name)

            break  # Not rate-limited, proceed

        # Handle authentication errors
        if resp.status_code == 401:
            self._log_error(endpoint.name, "Authentication failed (401)")
            raise FMPAuthenticationError()

        # Handle other HTTP errors
        if not resp.ok:
            error_msg = f"HTTP {resp.status_code}"
            if resp.status_code == 402:
                self._log_plan_limited(endpoint.name, error_msg)
            else:
                self._log_error(endpoint.name, error_msg)
            raise FMPAPIError(
                f"API error for '{endpoint.name}': {error_msg}",
                status_code=resp.status_code,
                endpoint=endpoint.name,
            )

        # Log successful request
        self._log_success(endpoint.name, response_time)

        if endpoint.response_type == "csv":
            text_payload = resp.text or ""
            if not text_payload.strip():
                return []
            reader = csv.DictReader(io.StringIO(text_payload))
            return [dict(row) for row in reader]

        return resp.json()

    def _log_rate_limit(self, endpoint_name: str) -> None:
        """Log rate limit hit."""
        try:
            from utils.logging import log_rate_limit_hit, log_service_health

            log_rate_limit_hit(None, endpoint_name, "api_calls", None, "free")
            log_service_health("FMP_API", "degraded", 0, {"error": "rate_limited"})
        except ImportError:
            pass

    def _log_success(self, endpoint_name: str, response_time: float) -> None:
        """Suppress healthy-call logs to keep output high signal."""
        _ = endpoint_name
        _ = response_time

    def _log_error(self, endpoint_name: str, error: str) -> None:
        """Log API error."""
        try:
            from utils.logging import log_critical_alert, log_service_health

            log_critical_alert(
                "fmp_api_error",
                "high",
                f"FMP API error for {endpoint_name}: {error}",
                "Check API status and retry",
            )
            log_service_health("FMP_API", "down", 0, {"error": error})
        except ImportError:
            pass

    def _log_plan_limited(self, endpoint_name: str, error: str) -> None:
        """Log entitlement/plan-limit errors (HTTP 402)."""
        try:
            from utils.logging import log_critical_alert, log_service_health

            log_critical_alert(
                "fmp_plan_limit",
                "medium",
                f"FMP plan limit for {endpoint_name}: {error}",
                "Use fallback provider or upgrade FMP plan",
            )
            log_service_health("FMP_API", "degraded", 0, {"error": error})
        except ImportError:
            pass

    def _build_cache_key(
        self,
        endpoint: FMPEndpoint,
        validated_params: dict[str, Any],
    ) -> list[str]:
        """
        Build cache key based on endpoint's cache_refresh strategy.

        Strategies:
        - MONTHLY: Add month token (refreshes each calendar month)
        - HASH_ONLY: Only use param hash (stable data, no auto-refresh)
          BUT: If endpoint has date params and 'to' is missing, add month token
          to prevent stale "latest data" cache hits
        - TTL: Use TTL-based expiration (handled by cache layer)
        """
        cache_key = [endpoint.name]

        # Check if endpoint has date params and 'to' is missing ("latest data" request)
        has_date_params = any(p.name in ("to", "from") for p in endpoint.params)
        has_end_date = any(
            k in validated_params and validated_params[k] is not None
            for k in ("to", "end_date", "to_date")
        )

        # Apply cache refresh strategy
        if endpoint.cache_refresh == CacheRefresh.MONTHLY:
            cache_key.append(f"month:{_get_month_token()}")
        elif endpoint.cache_refresh == CacheRefresh.HASH_ONLY:
            # For HASH_ONLY with date params but no end_date, add month token
            # to prevent stale "latest data" cache hits
            if has_date_params and not has_end_date:
                cache_key.append(f"month:{_get_month_token()}")
        # TTL doesn't add tokens - handled by cache layer

        # Add sorted params to key
        cache_key.extend(f"{k}:{v}" for k, v in sorted(validated_params.items()) if v is not None)

        return cache_key

    def fetch(
        self,
        endpoint_name: str,
        *,
        use_cache: bool = True,
        **params: Any,
    ) -> pd.DataFrame:
        """
        Fetch data from an FMP endpoint.

        Primary flow:
        1) Resolve endpoint metadata from ``fmp.registry``.
        2) Validate/coerce params via endpoint contract.
        3) Apply endpoint-specific cache key/refresh strategy.
        4) Execute HTTP request on miss and normalize response to DataFrame.

        Args:
            endpoint_name: Name of the registered endpoint
            use_cache: Whether to use disk caching (default True)
            **params: Endpoint parameters

        Returns:
            DataFrame with API response data

        Raises:
            FMPEndpointError: If endpoint is not registered
            FMPValidationError: If parameters are invalid
            FMPAPIError: If API request fails
            FMPEmptyResponseError: If API returns empty data
            FMPAuthenticationError: If API key is missing
        """
        endpoint = get_endpoint(endpoint_name)
        if endpoint is None:
            raise FMPEndpointError(endpoint_name)

        # Validate and build params
        try:
            validated_params = endpoint.build_params(**params)
        except ValueError as e:
            raise FMPValidationError(str(e))

        # Build cache key with staleness protection
        cache_key = self._build_cache_key(endpoint, validated_params)
        prefix = validated_params.get("symbol", endpoint_name)

        def _loader() -> pd.DataFrame:
            data = self._make_request(endpoint, validated_params)

            # Handle different response types
            if isinstance(data, dict):
                # Extract nested data using response_path (supports dot-path like "data.items")
                if endpoint.response_path:
                    extracted = _extract_by_path(data, endpoint.response_path)
                    if extracted is not None:
                        data = extracted
                    else:
                        # Path not found - log warning and try fallbacks
                        import warnings
                        warnings.warn(
                            f"response_path '{endpoint.response_path}' not found in response for "
                            f"endpoint '{endpoint_name}'. Trying fallbacks.",
                            UserWarning,
                            stacklevel=2,
                        )
                        # Fall through to fallback logic

                # Fallback: check for common nested keys (backward compatibility)
                if isinstance(data, dict):
                    if "historical" in data:
                        data = data["historical"]
                    elif endpoint.response_type == "object":
                        data = [data]
                    else:
                        data = [data]

            if not data:
                symbol = validated_params.get("symbol")
                raise FMPEmptyResponseError(endpoint_name, symbol)

            df = pd.DataFrame(data)

            # Apply response transform if defined (for complex response shaping)
            if endpoint.response_transform is not None:
                df = endpoint.response_transform(df)

            return df

        # Check if caching is disabled for this endpoint
        if not use_cache or not endpoint.cache_enabled:
            return _loader()

        return self.cache.read(
            key=cache_key,
            loader=_loader,
            cache_dir=endpoint.cache_dir,
            prefix=prefix,
            ttl_hours=endpoint.cache_ttl_hours,
        )

    def fetch_raw(
        self,
        endpoint_name: str,
        **params: Any,
    ) -> list | dict:
        """
        Fetch raw JSON response from an FMP endpoint (no caching).

        Args:
            endpoint_name: Name of the registered endpoint
            **params: Endpoint parameters

        Returns:
            Raw JSON response (list or dict)
        """
        endpoint = get_endpoint(endpoint_name)
        if endpoint is None:
            raise FMPEndpointError(endpoint_name)

        validated_params = endpoint.build_params(**params)
        return self._make_request(endpoint, validated_params)

    def list_endpoints(self, category: str | None = None) -> list[dict[str, str]]:
        """
        List available endpoints.

        Note: This method works without API key.

        Args:
            category: Optional category filter

        Returns:
            List of endpoint summaries
        """
        endpoints = registry_list_endpoints(category)
        return [
            {
                "name": e.name,
                "category": e.category,
                "description": e.description,
            }
            for e in endpoints
        ]

    def list_categories(self) -> list[str]:
        """Get all endpoint categories. Works without API key."""
        return get_categories()

    def describe(self, endpoint_name: str) -> dict[str, Any] | None:
        """
        Get full documentation for an endpoint.

        Note: This method works without API key.

        Args:
            endpoint_name: Name of the endpoint

        Returns:
            Dictionary with full endpoint details, or None if not found
        """
        endpoint = get_endpoint(endpoint_name)
        if endpoint is None:
            return None

        return {
            "name": endpoint.name,
            "path": endpoint.path,
            "description": endpoint.description,
            "category": endpoint.category,
            "fmp_docs_url": endpoint.fmp_docs_url,
            "cache_dir": endpoint.cache_dir,
            "cache_ttl_hours": endpoint.cache_ttl_hours,
            "cache_enabled": endpoint.cache_enabled,
            "parameters": [
                {
                    "name": p.name,
                    "type": p.param_type.value,
                    "required": p.required,
                    "default": p.default,
                    "description": p.description,
                    "enum_values": p.enum_values,
                }
                for p in endpoint.params
            ],
        }

    def generate_documentation(self) -> str:
        """
        Generate markdown documentation for all endpoints.

        Returns:
            Markdown string with full endpoint documentation
        """
        lines = ["# FMP Endpoints Reference", ""]

        for category in get_categories():
            lines.append(f"## {category.title()}")
            lines.append("")

            for endpoint in registry_list_endpoints(category):
                lines.append(f"### `{endpoint.name}`")
                lines.append("")
                lines.append(endpoint.description)
                lines.append("")

                if endpoint.fmp_docs_url:
                    lines.append(f"[FMP Documentation]({endpoint.fmp_docs_url})")
                    lines.append("")

                lines.append("**Parameters:**")
                lines.append("")
                lines.append("| Name | Type | Required | Default | Description |")
                lines.append("|------|------|----------|---------|-------------|")

                for p in endpoint.params:
                    req = "Yes" if p.required else "No"
                    default = p.default if p.default is not None else "-"
                    desc = p.description
                    if p.enum_values:
                        desc += f" (options: {', '.join(p.enum_values)})"
                    lines.append(f"| {p.name} | {p.param_type.value} | {req} | {default} | {desc} |")

                lines.append("")

                # Report cache strategy based on cache_refresh field
                if not endpoint.cache_enabled:
                    lines.append("*Cache: Disabled*")
                elif endpoint.cache_refresh == CacheRefresh.TTL:
                    lines.append(f"*Cache: TTL {endpoint.cache_ttl_hours} hours*")
                elif endpoint.cache_refresh == CacheRefresh.MONTHLY:
                    lines.append("*Cache: Monthly refresh*")
                else:  # HASH_ONLY
                    lines.append("*Cache: Hash-based (immutable data)*")
                lines.append("")

        return "\n".join(lines)


# Module-level convenience functions


@lru_cache(maxsize=1)
def get_client() -> FMPClient:
    """Get or create a shared FMP client instance."""
    return FMPClient()


def fetch(endpoint_name: str, **params: Any) -> pd.DataFrame:
    """
    Convenience function to fetch data using the shared client.

    Example:
        from fmp import fetch
        prices = fetch("historical_price_adjusted", symbol="AAPL")
    """
    return get_client().fetch(endpoint_name, **params)
