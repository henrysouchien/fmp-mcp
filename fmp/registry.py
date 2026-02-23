"""FMP Endpoint Registry.

Declarative endpoint definitions with full metadata for discoverability.
Each endpoint includes documentation URL, parameter specs, and caching config.

Agent orientation:
    Source of truth for FMP endpoint contracts (path, params, cache policy,
    response shaping). ``fmp.client.FMPClient`` should remain generic and depend
    on this metadata instead of hardcoding endpoint behavior.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable


class ParamType(Enum):
    """Parameter type enumeration for endpoint parameters."""

    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    DATE = "date"
    BOOLEAN = "boolean"
    ENUM = "enum"


# Common parameter aliases (user-friendly -> API name)
PARAM_ALIASES: dict[str, str] = {
    "from_date": "from",
    "to_date": "to",
    "start_date": "from",
    "end_date": "to",
}


@dataclass
class EndpointParam:
    """Definition of an endpoint parameter."""

    name: str
    param_type: ParamType
    required: bool = False
    default: Any = None
    description: str = ""
    enum_values: list[str] | None = None

    def validate(self, value: Any) -> Any:
        """Validate and coerce a parameter value."""
        if value is None:
            if self.required:
                raise ValueError(f"Required parameter '{self.name}' is missing")
            return self.default

        if self.param_type == ParamType.STRING:
            return str(value)
        elif self.param_type == ParamType.INTEGER:
            return int(value)
        elif self.param_type == ParamType.FLOAT:
            return float(value)
        elif self.param_type == ParamType.BOOLEAN:
            if isinstance(value, bool):
                return value
            return str(value).lower() in ("true", "1", "yes")
        elif self.param_type == ParamType.DATE:
            # Accept string dates as-is, coerce datetime objects
            if hasattr(value, "isoformat"):
                return value.isoformat()[:10]
            return str(value)[:10]
        elif self.param_type == ParamType.ENUM:
            str_val = str(value)
            if self.enum_values and str_val not in self.enum_values:
                raise ValueError(
                    f"Parameter '{self.name}' must be one of {self.enum_values}, got '{str_val}'"
                )
            return str_val

        return value


class CacheRefresh(Enum):
    """Cache refresh strategy for endpoints."""

    MONTHLY = "monthly"  # Add month token to cache key (refreshes each month)
    HASH_ONLY = "hash_only"  # Only use param hash (stable data, no auto-refresh)
    TTL = "ttl"  # Use TTL-based expiration (set cache_ttl_hours)


# Type alias for response transform function (DataFrame -> DataFrame)
# Using Any to avoid circular import with pandas
ResponseTransform = Callable[[Any], Any]


@dataclass
class FMPEndpoint:
    """Full endpoint definition with metadata.

    Contract notes:
    - ``build_params`` is the canonical param validation/coercion boundary.
    - ``cache_refresh`` + ``cache_ttl_hours`` define cache semantics used by
      ``fmp.client.FMPClient``.
    """

    name: str
    path: str
    description: str
    category: str
    params: list[EndpointParam] = field(default_factory=list)
    fmp_docs_url: str = ""
    cache_dir: str = "cache/fmp"
    cache_ttl_hours: int | None = None  # TTL in hours (required if cache_refresh=TTL)
    cache_enabled: bool = True  # Set False to disable caching (e.g., search)
    cache_refresh: CacheRefresh = CacheRefresh.HASH_ONLY  # Cache refresh strategy
    response_type: str = "list"  # "list" or "object"
    api_version: str = "stable"  # "stable" or "v3"
    response_path: str | None = None  # Dot-path to extract data (e.g., "historical", "data.items")
    response_transform: ResponseTransform | None = None  # Optional transform function for response shaping

    def __post_init__(self) -> None:
        """Validate endpoint configuration."""
        # Validate TTL configuration
        if self.cache_refresh == CacheRefresh.TTL and self.cache_ttl_hours is None:
            warnings.warn(
                f"Endpoint '{self.name}' has cache_refresh=TTL but no cache_ttl_hours set. "
                f"This will cache forever. Set cache_ttl_hours or use HASH_ONLY.",
                UserWarning,
                stacklevel=3,
            )

    def build_params(self, **kwargs: Any) -> dict[str, Any]:
        """Build and validate request parameters."""
        result = {}
        param_map = {p.name: p for p in self.params}

        # Apply parameter aliases (from_date -> from, etc.)
        aliased_kwargs = {}
        for key, value in kwargs.items():
            api_name = PARAM_ALIASES.get(key, key)
            if api_name != key and api_name in aliased_kwargs:
                # Both alias and real name provided - use real name
                continue
            aliased_kwargs[api_name] = value

        # Track unknown params for warning
        unknown_params = []

        # Validate provided parameters
        for key, value in aliased_kwargs.items():
            if key in param_map:
                validated = param_map[key].validate(value)
                if validated is not None:
                    result[key] = validated
            else:
                # Unknown parameter - warn but pass through for flexibility
                unknown_params.append(key)
                result[key] = value

        # Warn about unknown params
        if unknown_params:
            valid_params = [p.name for p in self.params]
            warnings.warn(
                f"Unknown parameters for endpoint '{self.name}': {unknown_params}. "
                f"Valid parameters: {valid_params}",
                UserWarning,
                stacklevel=3,
            )

        # Add defaults for missing optional params
        for param in self.params:
            if param.name not in result and param.default is not None:
                result[param.name] = param.default

        # Check required params
        for param in self.params:
            if param.required and param.name not in result:
                raise ValueError(f"Required parameter '{param.name}' is missing")

        return result


# Global endpoint registry
_ENDPOINTS: dict[str, FMPEndpoint] = {}


def register_endpoint(endpoint: FMPEndpoint) -> None:
    """Register an endpoint in the global registry."""
    _ENDPOINTS[endpoint.name] = endpoint


def get_endpoint(name: str) -> FMPEndpoint | None:
    """Get an endpoint by name."""
    return _ENDPOINTS.get(name)


def list_endpoints(category: str | None = None) -> list[FMPEndpoint]:
    """List all registered endpoints, optionally filtered by category."""
    endpoints = list(_ENDPOINTS.values())
    if category:
        endpoints = [e for e in endpoints if e.category == category]
    return sorted(endpoints, key=lambda e: (e.category, e.name))


def get_categories() -> list[str]:
    """Get all unique endpoint categories."""
    return sorted(set(e.category for e in _ENDPOINTS.values()))


# ============================================================================
# ENDPOINT REGISTRATIONS
# ============================================================================

# --- Prices ---

register_endpoint(
    FMPEndpoint(
        name="historical_price_eod",
        path="/historical-price-eod/full",
        description="End-of-day historical prices (open, high, low, close, volume)",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#historical-stock-price-end-of-day",
        category="prices",
        api_version="stable",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="Stock symbol"),
            EndpointParam("from", ParamType.DATE, description="Start date (YYYY-MM-DD)"),
            EndpointParam("to", ParamType.DATE, description="End date (YYYY-MM-DD)"),
            EndpointParam("serietype", ParamType.STRING, default="line", description="Series type"),
        ],
        cache_dir="cache/prices",
        cache_refresh=CacheRefresh.HASH_ONLY,  # Prices are immutable for given date range
    )
)

register_endpoint(
    FMPEndpoint(
        name="historical_price_adjusted",
        path="/historical-price-eod/dividend-adjusted",
        description="Dividend-adjusted historical prices (for total return calculations)",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#historical-stock-price-end-of-day",
        category="prices",
        api_version="stable",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="Stock symbol"),
            EndpointParam("from", ParamType.DATE, description="Start date (YYYY-MM-DD)"),
            EndpointParam("to", ParamType.DATE, description="End date (YYYY-MM-DD)"),
        ],
        cache_dir="cache/prices",
        cache_refresh=CacheRefresh.HASH_ONLY,  # Prices are immutable for given date range
    )
)

# --- Treasury ---

register_endpoint(
    FMPEndpoint(
        name="treasury_rates",
        path="/treasury-rates",
        description="US Treasury rates across multiple maturities",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#treasury-rates",
        category="treasury",
        api_version="stable",
        params=[
            EndpointParam("from", ParamType.DATE, description="Start date (YYYY-MM-DD)"),
            EndpointParam("to", ParamType.DATE, description="End date (YYYY-MM-DD)"),
        ],
        cache_dir="cache/prices",
        cache_refresh=CacheRefresh.HASH_ONLY,  # Historical rates are immutable
    )
)

# --- Dividends ---

register_endpoint(
    FMPEndpoint(
        name="dividends",
        path="/dividends",
        description="Dividend history (payment dates, amounts, frequency)",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#stock-dividend",
        category="dividends",
        api_version="stable",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="Stock symbol"),
        ],
        cache_dir="cache/dividends",
        cache_refresh=CacheRefresh.MONTHLY,  # New dividends declared monthly
    )
)

# --- Search ---

register_endpoint(
    FMPEndpoint(
        name="search",
        path="/search",
        description="Search for companies by name or ticker",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#ticker-search",
        category="search",
        api_version="v3",
        params=[
            EndpointParam("query", ParamType.STRING, required=True, description="Search query"),
            EndpointParam("limit", ParamType.INTEGER, default=10, description="Max results"),
            EndpointParam("exchange", ParamType.STRING, description="Filter by exchange"),
        ],
        cache_dir="cache/search",
        cache_enabled=False,  # Search results should not be cached
    )
)

register_endpoint(
    FMPEndpoint(
        name="profile",
        path="/profile/{symbol}",
        description="Company profile (name, sector, industry, description, currency)",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#company-profile",
        category="search",
        api_version="v3",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="Stock symbol"),
        ],
        cache_dir="cache/profile",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=168,  # 1 week
        response_type="list",  # Returns list with single item
    )
)

# --- Fundamentals ---
# Historical filings are immutable; use HASH_ONLY

register_endpoint(
    FMPEndpoint(
        name="income_statement",
        path="/income-statement",
        description="Income statement data (revenue, net income, EPS, margins)",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#income-statement",
        category="fundamentals",
        api_version="stable",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="Stock symbol"),
            EndpointParam(
                "period",
                ParamType.ENUM,
                default="annual",
                enum_values=["annual", "quarter"],
                description="Reporting period",
            ),
            EndpointParam("limit", ParamType.INTEGER, default=10, description="Number of periods"),
        ],
        cache_dir="cache/fundamentals",
        cache_refresh=CacheRefresh.HASH_ONLY,  # Historical filings don't change
    )
)

register_endpoint(
    FMPEndpoint(
        name="balance_sheet",
        path="/balance-sheet-statement",
        description="Balance sheet data (assets, liabilities, equity)",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#balance-sheet-statement",
        category="fundamentals",
        api_version="stable",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="Stock symbol"),
            EndpointParam(
                "period",
                ParamType.ENUM,
                default="annual",
                enum_values=["annual", "quarter"],
                description="Reporting period",
            ),
            EndpointParam("limit", ParamType.INTEGER, default=10, description="Number of periods"),
        ],
        cache_dir="cache/fundamentals",
        cache_refresh=CacheRefresh.HASH_ONLY,  # Historical filings don't change
    )
)

register_endpoint(
    FMPEndpoint(
        name="cash_flow",
        path="/cash-flow-statement",
        description="Cash flow statement data (operating, investing, financing)",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#cash-flow-statement",
        category="fundamentals",
        api_version="stable",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="Stock symbol"),
            EndpointParam(
                "period",
                ParamType.ENUM,
                default="annual",
                enum_values=["annual", "quarter"],
                description="Reporting period",
            ),
            EndpointParam("limit", ParamType.INTEGER, default=10, description="Number of periods"),
        ],
        cache_dir="cache/fundamentals",
        cache_refresh=CacheRefresh.HASH_ONLY,  # Historical filings don't change
    )
)

register_endpoint(
    FMPEndpoint(
        name="key_metrics",
        path="/key-metrics",
        description="Key financial metrics (P/E, P/B, ROE, debt ratios)",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#company-key-metrics",
        category="fundamentals",
        api_version="stable",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="Stock symbol"),
            EndpointParam(
                "period",
                ParamType.ENUM,
                default="annual",
                enum_values=["annual", "quarter"],
                description="Reporting period",
            ),
            EndpointParam("limit", ParamType.INTEGER, default=10, description="Number of periods"),
        ],
        cache_dir="cache/fundamentals",
        cache_refresh=CacheRefresh.HASH_ONLY,  # Historical filings don't change
    )
)

# --- Analyst ---
# Analyst data changes frequently; use TTL-based caching

register_endpoint(
    FMPEndpoint(
        name="analyst_estimates",
        path="/analyst-estimates",
        description="Analyst estimates (EPS, revenue forecasts)",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#analyst-estimates",
        category="analyst",
        api_version="stable",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="Stock symbol"),
            EndpointParam(
                "period",
                ParamType.ENUM,
                default="annual",
                enum_values=["annual", "quarter"],
                description="Estimate period",
            ),
            EndpointParam("limit", ParamType.INTEGER, default=10, description="Number of periods"),
        ],
        cache_dir="cache/analyst",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=24,  # Estimates change frequently
    )
)

register_endpoint(
    FMPEndpoint(
        name="price_target",
        path="/price-target-summary",
        description="Analyst price target summary (high, low, median, consensus)",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#price-target",
        category="analyst",
        api_version="stable",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="Stock symbol"),
        ],
        cache_dir="cache/analyst",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=24,
    )
)

register_endpoint(
    FMPEndpoint(
        name="price_target_consensus",
        path="/price-target-consensus",
        description="Analyst price target consensus",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#price-target",
        category="analyst",
        api_version="stable",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="Stock symbol"),
        ],
        cache_dir="cache/analyst",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=24,
    )
)

register_endpoint(
    FMPEndpoint(
        name="price_target_detail",
        path="/price-target",
        description="Individual analyst price target publications with dates and analyst firms",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#price-target",
        category="analyst",
        api_version="v4",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="Stock symbol"),
            EndpointParam("limit", ParamType.INTEGER, default=20, description="Max results"),
        ],
        cache_dir="cache/analyst",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=24,
    )
)

register_endpoint(
    FMPEndpoint(
        name="analyst_grades",
        path="/grades",
        description="Individual analyst upgrade/downgrade actions with dates and firms",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs/stable/grades",
        category="analyst",
        api_version="stable",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="Stock symbol"),
            EndpointParam("limit", ParamType.INTEGER, default=20, description="Max results"),
        ],
        cache_dir="cache/analyst",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=24,
    )
)

register_endpoint(
    FMPEndpoint(
        name="analyst_grades_historical",
        path="/grades-historical",
        description="Monthly snapshots of aggregate analyst rating distribution (buy/hold/sell counts)",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs/stable/grades-historical",
        category="analyst",
        api_version="stable",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="Stock symbol"),
            EndpointParam("limit", ParamType.INTEGER, default=12, description="Max monthly snapshots"),
        ],
        cache_dir="cache/analyst",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=24,
    )
)

register_endpoint(
    FMPEndpoint(
        name="earnings_surprises",
        path="/earnings-surprises/{symbol}",
        description="Historical actual vs estimated EPS at each earnings date",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#earnings-surprises",
        category="analyst",
        api_version="v3",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="Stock symbol"),
            EndpointParam("limit", ParamType.INTEGER, default=12, description="Max quarters"),
        ],
        cache_dir="cache/analyst",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=24,
    )
)

register_endpoint(
    FMPEndpoint(
        name="earnings_surprises_bulk",
        path="/earnings-surprises-bulk",
        description="Bulk earnings surprises (actual vs estimated EPS) for all symbols in a year",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#earnings-surprises",
        category="analyst",
        api_version="v4",
        params=[
            EndpointParam("year", ParamType.INTEGER, required=True, description="Calendar year"),
        ],
        cache_dir="cache/analyst",
        cache_refresh=CacheRefresh.MONTHLY,
        response_type="csv",
    )
)

# --- Transcripts ---
# Earnings call transcripts for qualitative context

register_endpoint(
    FMPEndpoint(
        name="earnings_transcript",
        path="/earning_call_transcript/{symbol}",
        description="Full earnings call transcript text for qualitative analysis",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#earnings-call-transcript",
        category="transcripts",
        api_version="v3",  # v3 API - stable requires premium
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="Stock symbol"),
            EndpointParam("year", ParamType.INTEGER, required=True, description="Fiscal year (e.g., 2024)"),
            EndpointParam("quarter", ParamType.INTEGER, required=True, description="Quarter (1-4)"),
        ],
        cache_dir="cache/transcripts",
        cache_refresh=CacheRefresh.HASH_ONLY,  # Transcripts are immutable once published
    )
)

# --- SEC Filings ---
# Official SEC filings (10-K, 10-Q, 8-K, etc.) for regulatory/accuracy context

# --- Screening ---

register_endpoint(
    FMPEndpoint(
        name="company_screener",
        path="/company-screener",
        description="Screen stocks by market cap, sector, beta, price, dividend, volume, country, exchange",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#stock-screener",
        category="screening",
        api_version="stable",
        params=[
            EndpointParam("marketCapMoreThan", ParamType.FLOAT, description="Min market cap (USD)"),
            EndpointParam("marketCapLowerThan", ParamType.FLOAT, description="Max market cap (USD)"),
            EndpointParam("sector", ParamType.STRING, description="Sector filter (e.g., Technology)"),
            EndpointParam("industry", ParamType.STRING, description="Industry filter (e.g., Software)"),
            EndpointParam("betaMoreThan", ParamType.FLOAT, description="Min beta"),
            EndpointParam("betaLowerThan", ParamType.FLOAT, description="Max beta"),
            EndpointParam("priceMoreThan", ParamType.FLOAT, description="Min stock price"),
            EndpointParam("priceLowerThan", ParamType.FLOAT, description="Max stock price"),
            EndpointParam("dividendMoreThan", ParamType.FLOAT, description="Min annual dividend"),
            EndpointParam("dividendLowerThan", ParamType.FLOAT, description="Max annual dividend"),
            EndpointParam("volumeMoreThan", ParamType.FLOAT, description="Min average volume"),
            EndpointParam("volumeLowerThan", ParamType.FLOAT, description="Max average volume"),
            EndpointParam("country", ParamType.STRING, description="Country filter (e.g., US)"),
            EndpointParam("exchange", ParamType.STRING, description="Exchange filter (e.g., NASDAQ)"),
            EndpointParam("isEtf", ParamType.BOOLEAN, description="Filter for ETFs only"),
            EndpointParam("isFund", ParamType.BOOLEAN, description="Filter for funds only"),
            EndpointParam("isActivelyTrading", ParamType.BOOLEAN, default=True, description="Only actively trading"),
            EndpointParam("limit", ParamType.INTEGER, default=50, description="Max results"),
        ],
        cache_dir="cache/screening",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=6,  # Screener data refreshes intraday
    )
)

# --- SEC Filings ---
# Official SEC filings (10-K, 10-Q, 8-K, etc.) for regulatory/accuracy context

register_endpoint(
    FMPEndpoint(
        name="sec_filings",
        path="/sec_filings/{symbol}",
        description="SEC filing metadata with links (10-K, 10-Q, 8-K, S-1, etc.)",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#sec-filings",
        category="filings",
        api_version="v3",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="Stock symbol"),
            EndpointParam(
                "type",
                ParamType.STRING,
                required=False,
                description="Filing type filter (10-K, 10-Q, 8-K, S-1, etc.)",
            ),
            EndpointParam("limit", ParamType.INTEGER, default=50, description="Number of filings to return"),
        ],
        cache_dir="cache/filings",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=24,  # New filings can appear daily
    )
)

# --- News ---
# News articles and press releases (time-sensitive, short TTL)

register_endpoint(
    FMPEndpoint(
        name="news_stock",
        path="/news/stock",
        description="Stock-specific news articles for given symbols",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#stock-news",
        category="news",
        api_version="stable",
        params=[
            EndpointParam("symbols", ParamType.STRING, required=True, description="Comma-separated symbols"),
            EndpointParam("from", ParamType.DATE, description="Start date (YYYY-MM-DD)"),
            EndpointParam("to", ParamType.DATE, description="End date (YYYY-MM-DD)"),
            EndpointParam("limit", ParamType.INTEGER, default=10, description="Max results"),
            EndpointParam("page", ParamType.INTEGER, default=0, description="Page number"),
        ],
        cache_dir="cache/news",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=1,  # News is time-sensitive
    )
)

register_endpoint(
    FMPEndpoint(
        name="news_general",
        path="/news/general-latest",
        description="Latest general market news",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#general-news",
        category="news",
        api_version="stable",
        params=[
            EndpointParam("limit", ParamType.INTEGER, default=10, description="Max results"),
            EndpointParam("page", ParamType.INTEGER, default=0, description="Page number"),
        ],
        cache_dir="cache/news",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=1,
    )
)

register_endpoint(
    FMPEndpoint(
        name="news_press_releases",
        path="/news/press-releases",
        description="Official company press releases",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#press-releases",
        category="news",
        api_version="stable",
        params=[
            EndpointParam("symbols", ParamType.STRING, required=True, description="Comma-separated symbols"),
            EndpointParam("from", ParamType.DATE, description="Start date (YYYY-MM-DD)"),
            EndpointParam("to", ParamType.DATE, description="End date (YYYY-MM-DD)"),
            EndpointParam("limit", ParamType.INTEGER, default=10, description="Max results"),
            EndpointParam("page", ParamType.INTEGER, default=0, description="Page number"),
        ],
        cache_dir="cache/news",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=1,
    )
)

# --- Calendars ---
# Corporate event calendars (earnings, dividends, splits, IPOs)

register_endpoint(
    FMPEndpoint(
        name="earnings_calendar",
        path="/earnings-calendar",
        description="Upcoming and recent earnings dates with estimates",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#earnings-calendar",
        category="calendar",
        api_version="stable",
        params=[
            EndpointParam("from", ParamType.DATE, description="Start date (YYYY-MM-DD)"),
            EndpointParam("to", ParamType.DATE, description="End date (YYYY-MM-DD)"),
        ],
        cache_dir="cache/calendar",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=6,  # Calendars update a few times per day
    )
)

register_endpoint(
    FMPEndpoint(
        name="dividends_calendar",
        path="/dividends-calendar",
        description="Upcoming ex-dividend dates and amounts",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#dividends-calendar",
        category="calendar",
        api_version="stable",
        params=[
            EndpointParam("from", ParamType.DATE, description="Start date (YYYY-MM-DD)"),
            EndpointParam("to", ParamType.DATE, description="End date (YYYY-MM-DD)"),
        ],
        cache_dir="cache/calendar",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=6,
    )
)

register_endpoint(
    FMPEndpoint(
        name="splits_calendar",
        path="/splits-calendar",
        description="Upcoming stock split dates",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#splits-calendar",
        category="calendar",
        api_version="stable",
        params=[
            EndpointParam("from", ParamType.DATE, description="Start date (YYYY-MM-DD)"),
            EndpointParam("to", ParamType.DATE, description="End date (YYYY-MM-DD)"),
        ],
        cache_dir="cache/calendar",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=6,
    )
)

register_endpoint(
    FMPEndpoint(
        name="ipos_calendar",
        path="/ipos-calendar",
        description="Upcoming IPO dates and pricing",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#ipos-calendar",
        category="calendar",
        api_version="stable",
        params=[
            EndpointParam("from", ParamType.DATE, description="Start date (YYYY-MM-DD)"),
            EndpointParam("to", ParamType.DATE, description="End date (YYYY-MM-DD)"),
        ],
        cache_dir="cache/calendar",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=6,
    )
)

# --- Peers ---

register_endpoint(
    FMPEndpoint(
        name="stock_peers",
        path="/stock-peers",
        description="Get peer companies for a stock (same sector, similar market cap)",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#stock-peers",
        category="screening",
        api_version="stable",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="Stock symbol"),
        ],
        cache_dir="cache/screening",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=168,  # Peers change slowly (1 week)
    )
)

register_endpoint(
    FMPEndpoint(
        name="shares_float",
        path="/shares-float",
        description="Public float, outstanding shares, and free float percentage (liquidity risk)",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs/stable/shares-float",
        category="screening",
        api_version="stable",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="Stock symbol"),
        ],
        cache_dir="cache/screening",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=168,  # Float changes infrequently (1 week)
    )
)

register_endpoint(
    FMPEndpoint(
        name="ratios_ttm",
        path="/ratios-ttm",
        description="Trailing twelve month financial ratios (P/E, ROE, margins, leverage)",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#company-financial-ratios",
        category="fundamentals",
        api_version="stable",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="Stock symbol"),
        ],
        cache_dir="cache/fundamentals",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=24,  # TTM ratios update daily
    )
)

# --- Market Movers ---

register_endpoint(
    FMPEndpoint(
        name="biggest_gainers",
        path="/biggest-gainers",
        description="Top gaining stocks by daily percentage change",
        category="market_movers",
        api_version="stable",
        cache_dir="cache/market",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=1,
    )
)

register_endpoint(
    FMPEndpoint(
        name="biggest_losers",
        path="/biggest-losers",
        description="Top losing stocks by daily percentage change",
        category="market_movers",
        api_version="stable",
        cache_dir="cache/market",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=1,
    )
)

register_endpoint(
    FMPEndpoint(
        name="most_actives",
        path="/most-actives",
        description="Most actively traded stocks by volume",
        category="market_movers",
        api_version="stable",
        cache_dir="cache/market",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=1,
    )
)

# --- Quotes ---

register_endpoint(
    FMPEndpoint(
        name="batch_index_quotes",
        path="/batch-index-quotes",
        description="Batch quotes for market indices",
        category="quotes",
        api_version="stable",
        params=[
            EndpointParam("short", ParamType.BOOLEAN, description="Short format response"),
        ],
        cache_dir="cache/quotes",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=1,
    )
)

# --- Macro / Economic ---

# --- Sector ---

register_endpoint(
    FMPEndpoint(
        name="sector_performance_snapshot",
        path="/sector-performance-snapshot",
        description="Daily sector percentage change snapshot",
        category="sector",
        api_version="stable",
        params=[
            EndpointParam("date", ParamType.DATE, description="Snapshot date (YYYY-MM-DD)"),
        ],
        cache_dir="cache/sector",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=6,
    )
)

register_endpoint(
    FMPEndpoint(
        name="industry_performance_snapshot",
        path="/industry-performance-snapshot",
        description="Daily industry percentage change snapshot",
        category="sector",
        api_version="stable",
        params=[
            EndpointParam("date", ParamType.DATE, description="Snapshot date (YYYY-MM-DD)"),
        ],
        cache_dir="cache/sector",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=6,
    )
)

register_endpoint(
    FMPEndpoint(
        name="sector_pe_snapshot",
        path="/sector-pe-snapshot",
        description="Sector aggregate P/E ratio snapshot",
        category="sector",
        api_version="stable",
        params=[
            EndpointParam("date", ParamType.DATE, description="Snapshot date (YYYY-MM-DD)"),
        ],
        cache_dir="cache/sector",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=6,
    )
)

register_endpoint(
    FMPEndpoint(
        name="industry_pe_snapshot",
        path="/industry-pe-snapshot",
        description="Industry aggregate P/E ratio snapshot",
        category="sector",
        api_version="stable",
        params=[
            EndpointParam("date", ParamType.DATE, description="Snapshot date (YYYY-MM-DD)"),
        ],
        cache_dir="cache/sector",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=6,
    )
)

# --- Macro / Economic ---

register_endpoint(
    FMPEndpoint(
        name="economic_indicators",
        path="/economic-indicators",
        description="Economic indicator time series (GDP, CPI, unemployment, etc.)",
        category="macro",
        api_version="stable",
        params=[
            EndpointParam("name", ParamType.STRING, required=True,
                          description="Indicator name (GDP, CPI, federalFunds, unemploymentRate, etc.)"),
            EndpointParam("from", ParamType.DATE, description="Start date (YYYY-MM-DD)"),
            EndpointParam("to", ParamType.DATE, description="End date (YYYY-MM-DD)"),
        ],
        cache_dir="cache/macro",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=24,  # Economic data updates infrequently but worth refreshing daily
    )
)

register_endpoint(
    FMPEndpoint(
        name="economic_calendar",
        path="/economic-calendar",
        description="Upcoming/recent economic events with prior/forecast/actual values",
        category="macro",
        api_version="stable",
        params=[
            EndpointParam("from", ParamType.DATE, description="Start date (YYYY-MM-DD)"),
            EndpointParam("to", ParamType.DATE, description="End date (YYYY-MM-DD)"),
        ],
        cache_dir="cache/macro",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=6,  # Calendar events update throughout the day
    )
)

# --- Institutional Ownership ---
# 13F ownership and holder analytics; refresh daily

register_endpoint(
    FMPEndpoint(
        name="institutional_holders",
        path="/institutional-ownership/extract-analytics/holder",
        description="Institutional holders with share change and portfolio weight analytics",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs/stable/institutional-ownership-by-holder",
        category="institutional",
        api_version="stable",
        response_type="list",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="Stock symbol"),
            EndpointParam("year", ParamType.INTEGER, description="Filing year"),
            EndpointParam("quarter", ParamType.INTEGER, description="Filing quarter (1-4)"),
            EndpointParam("page", ParamType.INTEGER, default=0, description="Page number"),
            EndpointParam("limit", ParamType.INTEGER, default=20, description="Max rows per page"),
        ],
        cache_dir="cache/institutional",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=24,
    )
)

register_endpoint(
    FMPEndpoint(
        name="institutional_positions_summary",
        path="/institutional-ownership/symbol-positions-summary",
        description="Aggregate institutional ownership summary for a symbol",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs/stable/institutional-ownership-positions-summary",
        category="institutional",
        api_version="stable",
        response_type="list",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="Stock symbol"),
            EndpointParam("year", ParamType.INTEGER, description="Filing year"),
            EndpointParam("quarter", ParamType.INTEGER, description="Filing quarter (1-4)"),
        ],
        cache_dir="cache/institutional",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=24,
    )
)

register_endpoint(
    FMPEndpoint(
        name="institutional_holder_performance",
        path="/institutional-ownership/holder-performance-summary",
        description="Performance summary for an institutional holder (CIK)",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs/stable/holder-performance-summary",
        category="institutional",
        api_version="stable",
        response_type="list",
        params=[
            EndpointParam("cik", ParamType.STRING, required=True, description="Institution CIK"),
            EndpointParam("page", ParamType.INTEGER, default=0, description="Page number"),
        ],
        cache_dir="cache/institutional",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=24,
    )
)

register_endpoint(
    FMPEndpoint(
        name="institutional_industry_breakdown",
        path="/institutional-ownership/holder-industry-breakdown",
        description="Industry-level breakdown for an institutional holder",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs/stable/holder-industry-breakdown",
        category="institutional",
        api_version="stable",
        response_type="list",
        params=[
            EndpointParam("cik", ParamType.STRING, required=True, description="Institution CIK"),
            EndpointParam("year", ParamType.INTEGER, description="Filing year"),
            EndpointParam("quarter", ParamType.INTEGER, description="Filing quarter (1-4)"),
        ],
        cache_dir="cache/institutional",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=24,
    )
)

register_endpoint(
    FMPEndpoint(
        name="institutional_filings_dates",
        path="/institutional-ownership/dates",
        description="Available 13F filing dates for an institutional holder",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs/stable/institutional-ownership-dates",
        category="institutional",
        api_version="stable",
        response_type="list",
        params=[
            EndpointParam("cik", ParamType.STRING, required=True, description="Institution CIK"),
        ],
        cache_dir="cache/institutional",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=24,
    )
)

# --- Institutional Ownership (v3 legacy fallback) ---
# v3 endpoints accessible on lower-tier plans; stable versions require higher tier

register_endpoint(
    FMPEndpoint(
        name="institutional_holders_v3",
        path="/institutional-holder/{symbol}",
        description="Institutional holders for a stock (v3 legacy — broader plan access)",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs/institutional-holders-api",
        category="institutional",
        api_version="v3",
        response_type="list",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="Stock symbol"),
        ],
        cache_dir="cache/institutional",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=24,
    )
)

# --- ETF Holdings (v3 legacy fallback) ---

register_endpoint(
    FMPEndpoint(
        name="etf_holdings_v3",
        path="/etf-holder/{symbol}",
        description="ETF holdings with constituent weights (v3 legacy — broader plan access)",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs/etf-holders-api",
        category="etf",
        api_version="v3",
        response_type="list",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="ETF symbol"),
        ],
        cache_dir="cache/etf",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=24,
    )
)

# --- Insider Trading ---
# Insider transaction flow and statistics; refresh intraday

register_endpoint(
    FMPEndpoint(
        name="insider_trades_search",
        path="/insider-trading/search",
        description="Search insider trades by symbol with pagination",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs/stable/insider-trading-search",
        category="insider",
        api_version="stable",
        response_type="list",
        params=[
            EndpointParam("symbol", ParamType.STRING, description="Stock symbol"),
            EndpointParam("page", ParamType.INTEGER, default=0, description="Page number"),
            EndpointParam("limit", ParamType.INTEGER, default=20, description="Max rows per page"),
        ],
        cache_dir="cache/insider",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=6,
    )
)

register_endpoint(
    FMPEndpoint(
        name="insider_trade_statistics",
        path="/insider-trading/statistics",
        description="Insider trading statistics for a symbol",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs/stable/insider-trading-statistics",
        category="insider",
        api_version="stable",
        response_type="list",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="Stock symbol"),
        ],
        cache_dir="cache/insider",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=6,
    )
)

register_endpoint(
    FMPEndpoint(
        name="insider_transaction_types",
        path="/insider-trading-transaction-type",
        description="Reference list of insider transaction types",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs/stable/insider-trading-transaction-types",
        category="insider",
        api_version="stable",
        response_type="list",
        cache_dir="cache/insider",
        cache_refresh=CacheRefresh.HASH_ONLY,
    )
)

# --- ETF / Funds ---
# ETF/fund composition and allocation snapshots; refresh daily

register_endpoint(
    FMPEndpoint(
        name="etf_holdings",
        path="/etf/holdings",
        description="ETF holdings with constituent weights",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs/stable/etf-holdings",
        category="etf",
        api_version="stable",
        response_type="list",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="ETF symbol"),
        ],
        cache_dir="cache/etf",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=24,
    )
)

register_endpoint(
    FMPEndpoint(
        name="etf_info",
        path="/etf/info",
        description="ETF metadata and fund-level details",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs/stable/etf-info",
        category="etf",
        api_version="stable",
        response_type="list",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="ETF symbol"),
        ],
        cache_dir="cache/etf",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=24,
    )
)

register_endpoint(
    FMPEndpoint(
        name="etf_country_weightings",
        path="/etf/country-weightings",
        description="ETF country allocation weightings",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs/stable/etf-country-weightings",
        category="etf",
        api_version="stable",
        response_type="list",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="ETF symbol"),
        ],
        cache_dir="cache/etf",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=24,
    )
)

register_endpoint(
    FMPEndpoint(
        name="etf_sector_weightings",
        path="/etf/sector-weightings",
        description="ETF sector allocation weightings",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs/stable/etf-sector-weightings",
        category="etf",
        api_version="stable",
        response_type="list",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="ETF symbol"),
        ],
        cache_dir="cache/etf",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=24,
    )
)

register_endpoint(
    FMPEndpoint(
        name="etf_asset_exposure",
        path="/etf/asset-exposure",
        description="ETF asset exposure breakdown",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs/stable/etf-asset-exposure",
        category="etf",
        api_version="stable",
        response_type="list",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="ETF symbol"),
        ],
        cache_dir="cache/etf",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=24,
    )
)

register_endpoint(
    FMPEndpoint(
        name="etf_disclosure",
        path="/funds/disclosure-holders-latest",
        description="Latest fund disclosure holders for an ETF/fund symbol",
        fmp_docs_url="https://site.financialmodelingprep.com/developer/docs/stable/disclosure-holders-latest",
        category="etf",
        api_version="stable",
        response_type="list",
        params=[
            EndpointParam("symbol", ParamType.STRING, required=True, description="ETF symbol"),
        ],
        cache_dir="cache/etf",
        cache_refresh=CacheRefresh.TTL,
        cache_ttl_hours=24,
    )
)

# --- Technical Indicators ---
# Intraday data changes frequently; use TTL-based caching

_TIMEFRAME_VALUES = ["1min", "5min", "15min", "30min", "1hour", "4hour", "1day"]

for _ta_name, _ta_path, _ta_desc in [
    ("ta_sma", "/technical-indicators/sma", "Simple Moving Average"),
    ("ta_ema", "/technical-indicators/ema", "Exponential Moving Average"),
    ("ta_rsi", "/technical-indicators/rsi", "Relative Strength Index (0-100)"),
    ("ta_adx", "/technical-indicators/adx", "Average Directional Index (trend strength)"),
    ("ta_williams", "/technical-indicators/williams", "Williams %R (-100 to 0)"),
    ("ta_stddev", "/technical-indicators/standarddeviation", "Standard Deviation (volatility)"),
]:
    register_endpoint(
        FMPEndpoint(
            name=_ta_name,
            path=_ta_path,
            description=_ta_desc,
            fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#technical-indicators",
            category="technical",
            api_version="stable",
            params=[
                EndpointParam("symbol", ParamType.STRING, required=True, description="Stock symbol"),
                EndpointParam("periodLength", ParamType.INTEGER, default=14, description="Indicator period"),
                EndpointParam(
                    "timeframe",
                    ParamType.ENUM,
                    default="1day",
                    enum_values=_TIMEFRAME_VALUES,
                    description="Candle timeframe",
                ),
                EndpointParam("from", ParamType.DATE, description="Start date (YYYY-MM-DD)"),
                EndpointParam("to", ParamType.DATE, description="End date (YYYY-MM-DD)"),
            ],
            cache_dir="cache/technical",
            cache_refresh=CacheRefresh.TTL,
            cache_ttl_hours=4,
        )
    )
