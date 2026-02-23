"""FMP Data Abstraction Layer.

Unified interface for Financial Modeling Prep (FMP) API data access.

Agent orientation:
    Public package entrypoint for FMP-backed market/fundamental data access.
    Start with ``fmp.client.FMPClient`` for fetch behavior and ``fmp.registry``
    for endpoint metadata/contracts.

Primary callers:
    - Core risk/performance workflows through ``data_loader`` compatibility paths
    - Service/API layers needing discoverable FMP endpoints

Features:
- Discoverable endpoints with full metadata
- Disk caching with per-endpoint refresh strategies (HASH_ONLY, MONTHLY, TTL)
- Structured error handling
- Backward-compatible wrappers

Quick Start:
    from fmp import FMPClient

    fmp = FMPClient()

    # Fetch data
    prices = fmp.fetch("historical_price_adjusted", symbol="AAPL")
    income = fmp.fetch("income_statement", symbol="AAPL", period="quarter")

    # Discover endpoints
    fmp.list_endpoints()                    # All endpoints
    fmp.list_endpoints(category="analyst")  # Filter by category
    fmp.describe("income_statement")        # Full documentation

Convenience Functions:
    from fmp import fetch, get_client

    prices = fetch("historical_price_adjusted", symbol="AAPL")
    client = get_client()  # Shared client instance

Backward Compatibility:
    from fmp.compat import fetch_monthly_close, fetch_monthly_total_return_price

Available Endpoints:
    - prices: historical_price_eod, historical_price_adjusted
    - treasury: treasury_rates
    - dividends: dividends
    - search: search, profile
    - fundamentals: income_statement, balance_sheet, cash_flow, key_metrics
    - analyst: analyst_estimates, price_target
"""

from .client import FMPClient, fetch, get_client
try:
    from .estimate_store import EstimateStore
except ImportError:
    EstimateStore = None
from .exceptions import (
    FMPAPIError,
    FMPAuthenticationError,
    FMPEmptyResponseError,
    FMPEndpointError,
    FMPError,
    FMPRateLimitError,
    FMPValidationError,
)
from .registry import (
    CacheRefresh,
    EndpointParam,
    FMPEndpoint,
    ParamType,
    get_categories,
    get_endpoint,
    list_endpoints,
    register_endpoint,
)

__all__ = [
    # Client
    "FMPClient",
    "fetch",
    "get_client",
    "EstimateStore",
    # Exceptions
    "FMPError",
    "FMPAPIError",
    "FMPAuthenticationError",
    "FMPEmptyResponseError",
    "FMPEndpointError",
    "FMPRateLimitError",
    "FMPValidationError",
    # Registry
    "FMPEndpoint",
    "EndpointParam",
    "ParamType",
    "CacheRefresh",
    "register_endpoint",
    "get_endpoint",
    "list_endpoints",
    "get_categories",
]
