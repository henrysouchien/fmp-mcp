#!/usr/bin/env python3
"""
FMP MCP Server

MCP (Model Context Protocol) server for FMP (Financial Modeling Prep) API.
Exposes financial data tools for AI assistant invocation.

Setup:
    pip install fmp-mcp
    claude mcp add fmp-mcp -- fmp-mcp

Usage:
    From Claude: "Get Apple's income statement"
    From Claude: "What FMP endpoints are available?"
    From Claude: "Search for semiconductor companies"
"""

# CRITICAL: Redirect stdout to stderr BEFORE any imports
# MCP uses stdout for JSON-RPC - all other output (logs, prints) must go to stderr
import sys

_real_stdout = sys.stdout  # Save for MCP
sys.stdout = sys.stderr  # All prints/logs now go to stderr

from typing import Literal, Optional

from fastmcp import FastMCP

from fmp.tools.fmp_core import (
    fmp_describe as _fmp_describe,
    fmp_fetch as _fmp_fetch,
    fmp_list_endpoints as _fmp_list_endpoints,
    fmp_profile as _fmp_profile,
    fmp_search as _fmp_search,
)
from fmp.tools.screening import screen_stocks as _screen_stocks
from fmp.tools.peers import compare_peers as _compare_peers
from fmp.tools.market import get_economic_data as _get_economic_data
from fmp.tools.market import get_sector_overview as _get_sector_overview
from fmp.tools.market import get_market_context as _get_market_context
from fmp.tools.institutional import get_institutional_ownership as _get_institutional_ownership
from fmp.tools.insider import get_insider_trades as _get_insider_trades
from fmp.tools.etf_funds import get_etf_holdings as _get_etf_holdings
from fmp.tools.news_events import get_news as _get_news
from fmp.tools.news_events import get_events_calendar as _get_events_calendar
from fmp.tools.technical import get_technical_analysis as _get_technical_analysis
from fmp.tools.transcripts import get_earnings_transcript as _get_earnings_transcript

try:
    from fmp.tools.estimates import (
        get_estimate_revisions as _get_estimate_revisions,
        screen_estimate_revisions as _screen_estimate_revisions,
    )
    _HAS_ESTIMATES = True
except ImportError:
    _HAS_ESTIMATES = False

# Restore stdout for MCP communication (logs still go to stderr)
sys.stdout = _real_stdout

# Initialize FastMCP server
mcp = FastMCP(
    "fmp-mcp",
    instructions="""FMP (Financial Modeling Prep) financial data API tools.

Use these tools to fetch financial data:
- fmp_list_endpoints: Discover available data endpoints
- fmp_describe: Get endpoint parameter documentation
- fmp_fetch: Fetch data from any endpoint
- fmp_search: Search for companies by name
- fmp_profile: Get company profile details
- get_estimate_revisions: Get historical estimate revisions for one ticker/fiscal period
- screen_estimate_revisions: Screen tickers for up/down estimate momentum
- screen_stocks: Screen stocks by fundamental criteria
- compare_peers: Compare a stock against its peers on financial ratios
- get_technical_analysis: Get composite technical analysis (trend, momentum, volatility signals)
- get_economic_data: Get economic indicators and calendar events
- get_sector_overview: Get sector/industry performance and P/E valuation overview
- get_market_context: Get a market snapshot across indices, sectors, movers, and events
- get_institutional_ownership: Get institutional holder analytics and ownership summary for a stock
- get_insider_trades: Get insider trade flow and insider statistics for a stock
- get_etf_holdings: Get ETF/fund holdings, sector/country allocation, and metadata in one call
- get_news: Fetch news articles for stocks or the broad market
- get_events_calendar: Fetch corporate event calendars (earnings, dividends, splits, IPOs)
- get_earnings_transcript: Parse and navigate earnings call transcripts (prepared remarks, Q&A, per-speaker)

Workflow: Use fmp_list_endpoints to discover endpoints, fmp_describe to understand
parameters, then fmp_fetch to retrieve data. Use screen_stocks, get_news, and
get_events_calendar for higher-level market data queries. Use get_market_context
for a one-call morning market briefing.""",
)


@mcp.tool()
def fmp_fetch(
    endpoint: str,
    symbol: Optional[str] = None,
    period: Optional[Literal["annual", "quarter"]] = None,
    limit: Optional[int] = None,
    columns: Optional[list[str]] = None,
    output: Literal["inline", "file"] = "inline",
    use_cache: bool = True,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    query: Optional[str] = None,
    year: Optional[int] = None,
    quarter: Optional[int] = None,
    type: Optional[str] = None,
) -> dict:
    """
    Fetch data from any registered FMP endpoint.

    This is the primary data fetching tool. Use fmp_list_endpoints to discover
    available endpoints, and fmp_describe to see required parameters.

    Args:
        endpoint: Name of the FMP endpoint. Common endpoints:
            - income_statement: Income statement data
            - balance_sheet: Balance sheet data
            - cash_flow: Cash flow statement data
            - key_metrics: Financial ratios and metrics
            - historical_price_adjusted: Adjusted stock prices
            - dividends: Dividend history
            - analyst_estimates: Analyst EPS/revenue estimates
            - price_target: Analyst price targets
            - earnings_transcript: Earnings call transcripts
            - sec_filings: SEC filing metadata
        symbol: Stock symbol (e.g., "AAPL", "MSFT"). Required for most endpoints.
        period: Reporting period for financial statements ("annual" or "quarter").
        limit: Maximum number of records to return.
        columns: Optional list of column names to keep in output.
        output: Output mode: inline records or CSV file path.
        use_cache: Whether to use cached data (default: True). Set False for fresh data.
        from_date: Start date for price/rate data (YYYY-MM-DD format).
        to_date: End date for price/rate data (YYYY-MM-DD format).
        query: Search query (for search endpoint).
        year: Fiscal year (for earnings_transcript endpoint).
        quarter: Quarter 1-4 (for earnings_transcript endpoint).
        type: Filing type filter (for sec_filings endpoint, e.g., "10-K", "10-Q").

    Returns:
        dict with status, endpoint, params, row_count, columns, and data (list of records).
        On error: status="error" with error_type and message.

    Examples:
        # Get Apple's last 3 annual income statements
        fmp_fetch(endpoint="income_statement", symbol="AAPL", period="annual", limit=3)

        # Get adjusted prices for date range
        fmp_fetch(endpoint="historical_price_adjusted", symbol="AAPL", from_date="2024-01-01")

        # Get Q4 2024 earnings transcript
        fmp_fetch(endpoint="earnings_transcript", symbol="AAPL", year=2024, quarter=4)
    """
    # Build kwargs dict with all optional parameters
    kwargs = {}
    if from_date:
        kwargs["from_date"] = from_date
    if to_date:
        kwargs["to_date"] = to_date
    if query:
        kwargs["query"] = query
    if year:
        kwargs["year"] = year
    if quarter:
        kwargs["quarter"] = quarter
    if type:
        kwargs["type"] = type

    return _fmp_fetch(
        endpoint=endpoint,
        symbol=symbol,
        period=period,
        limit=limit,
        columns=columns,
        output=output,
        use_cache=use_cache,
        **kwargs,
    )


@mcp.tool()
def fmp_search(
    query: str,
    limit: int = 10,
    exchange: Optional[str] = None,
) -> dict:
    """
    Search for companies by name or ticker.

    Convenience wrapper for company search. Returns matching companies
    with their symbols, names, and exchange information.

    Args:
        query: Search query (company name, partial name, or ticker).
        limit: Maximum number of results (default: 10, max: 100).
        exchange: Filter by exchange (e.g., "NASDAQ", "NYSE", "AMEX").

    Returns:
        dict with status, query, result_count, and results list.
        Each result contains symbol, name, currency, stockExchange, exchangeShortName.

    Examples:
        fmp_search(query="apple")
        fmp_search(query="semiconductor", limit=20)
        fmp_search(query="bank", exchange="NYSE")
    """
    return _fmp_search(query=query, limit=limit, exchange=exchange)


@mcp.tool()
def fmp_profile(symbol: str) -> dict:
    """
    Get detailed company profile information.

    Returns comprehensive company data including sector, industry,
    description, CEO, employees, website, and key financial metrics.

    Args:
        symbol: Stock symbol (e.g., "AAPL", "MSFT", "GOOGL").

    Returns:
        dict with status, symbol, and profile containing:
        - companyName, symbol, exchange, currency
        - sector, industry, description
        - ceo, fullTimeEmployees, website
        - price, mktCap, beta, volAvg
        - dcf, dcfDiff (discounted cash flow valuation)
        - ipoDate, address, city, state, country

    Examples:
        fmp_profile(symbol="AAPL")
        fmp_profile(symbol="MSFT")
    """
    return _fmp_profile(symbol=symbol)


@mcp.tool()
def fmp_list_endpoints(category: Optional[str] = None) -> dict:
    """
    List available FMP data endpoints.

    Discovery tool to see what data is available. Use this first
    to understand what endpoints exist before fetching data.

    Args:
        category: Filter by category. Available categories:
            - prices: Historical stock prices
            - treasury: Treasury rates
            - dividends: Dividend history
            - search: Company search and profiles
            - fundamentals: Financial statements (income, balance, cash flow)
            - analyst: Analyst estimates and price targets
            - transcripts: Earnings call transcripts
            - filings: SEC filing metadata

    Returns:
        dict with status, categories list, endpoint_count, and endpoints list.
        Each endpoint has name, category, and description.

    Examples:
        fmp_list_endpoints()  # All endpoints
        fmp_list_endpoints(category="fundamentals")  # Just financials
    """
    return _fmp_list_endpoints(category=category)


@mcp.tool()
def fmp_describe(endpoint: str) -> dict:
    """
    Get detailed documentation for an FMP endpoint.

    Discovery tool to understand what parameters an endpoint accepts.
    Use this before calling fmp_fetch to know required vs optional params.

    Args:
        endpoint: Name of the endpoint (e.g., "income_statement", "historical_price_adjusted").

    Returns:
        dict with status, endpoint, and documentation containing:
        - name, path, description, category
        - fmp_docs_url: Link to official FMP documentation
        - cache_dir, cache_ttl_hours, cache_enabled
        - parameters: List with name, type, required, default, description, enum_values

    Examples:
        fmp_describe(endpoint="income_statement")
        fmp_describe(endpoint="historical_price_adjusted")
    """
    return _fmp_describe(endpoint=endpoint)


@mcp.tool()
def screen_stocks(
    sector: Optional[str] = None,
    industry: Optional[str] = None,
    market_cap_min: Optional[float] = None,
    market_cap_max: Optional[float] = None,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    dividend_min: Optional[float] = None,
    dividend_max: Optional[float] = None,
    beta_min: Optional[float] = None,
    beta_max: Optional[float] = None,
    volume_min: Optional[float] = None,
    volume_max: Optional[float] = None,
    country: Optional[str] = None,
    exchange: Optional[str] = None,
    is_etf: Optional[bool] = None,
    is_fund: Optional[bool] = None,
    limit: int = 50,
    format: Literal["full", "summary"] = "summary",
) -> dict:
    """
    Screen stocks by fundamental criteria (sector, market cap, beta, dividend, etc.).

    Searches the full universe of stocks and ETFs using financial filters.
    Combine multiple criteria to narrow results.

    Args:
        sector: Sector filter (e.g., "Technology", "Healthcare", "Energy").
        industry: Industry filter (e.g., "Software", "Biotechnology").
        market_cap_min: Minimum market capitalization in USD (e.g., 10000000000 for $10B).
        market_cap_max: Maximum market capitalization in USD.
        price_min: Minimum stock price.
        price_max: Maximum stock price.
        dividend_min: Minimum annual dividend per share.
        dividend_max: Maximum annual dividend per share.
        beta_min: Minimum beta (market sensitivity).
        beta_max: Maximum beta.
        volume_min: Minimum average daily volume.
        volume_max: Maximum average daily volume.
        country: Country filter (e.g., "US", "GB", "JP").
        exchange: Exchange filter (e.g., "NASDAQ", "NYSE", "LSE").
        is_etf: Set to true to screen ETFs only, false for stocks only.
        is_fund: Set to true to screen funds only, false to exclude funds.
        limit: Maximum number of results (default: 50).
        format: Output format:
            - "summary": Key metrics per result (symbol, name, sector, market cap, price, beta)
            - "full": All available fields from the screener

    Returns:
        Screening results with status field ("success" or "error").

    Examples:
        "Find large-cap tech stocks" -> screen_stocks(sector="Technology", market_cap_min=10000000000)
        "Low beta dividend stocks" -> screen_stocks(beta_max=0.8, dividend_min=2.0)
        "Show me biotech stocks under $50" -> screen_stocks(industry="Biotechnology", price_max=50)
        "Screen for ETFs" -> screen_stocks(is_etf=True)
        "High volume NASDAQ stocks" -> screen_stocks(exchange="NASDAQ", volume_min=5000000)
        "Screen for mutual funds" -> screen_stocks(is_fund=True)
    """
    return _screen_stocks(
        sector=sector,
        industry=industry,
        market_cap_min=market_cap_min,
        market_cap_max=market_cap_max,
        price_min=price_min,
        price_max=price_max,
        dividend_min=dividend_min,
        dividend_max=dividend_max,
        beta_min=beta_min,
        beta_max=beta_max,
        volume_min=volume_min,
        volume_max=volume_max,
        country=country,
        exchange=exchange,
        is_etf=is_etf,
        is_fund=is_fund,
        limit=limit,
        format=format,
    )


@mcp.tool()
def get_estimate_revisions(
    ticker: str,
    fiscal_date: Optional[str] = None,
    period: Literal["quarter", "annual"] = "quarter",
) -> dict:
    """
    Get estimate revision history for one ticker and fiscal period.

    Reads snapshots from the local estimate store populated by the monthly
    collection job. If `fiscal_date` is omitted, defaults to the nearest
    available upcoming fiscal period for the ticker.

    Args:
        ticker: Ticker symbol (e.g., "AAPL", "MSFT").
        fiscal_date: Fiscal period date (YYYY-MM-DD). Optional.
        period: Estimate horizon:
            - "quarter": Quarterly estimates (default)
            - "annual": Annual estimates

    Returns:
        Revision timeline with snapshot-by-snapshot values and summary deltas.
    """
    if not _HAS_ESTIMATES:
        return {
            "status": "error",
            "error": "Estimate tools unavailable: install optional dependency with fmp-mcp[estimates].",
        }
    return _get_estimate_revisions(
        ticker=ticker,
        fiscal_date=fiscal_date,
        period=period,
    )


@mcp.tool()
def screen_estimate_revisions(
    tickers: Optional[list[str]] = None,
    days: int = 30,
    direction: Literal["up", "down", "all"] = "all",
    period: Literal["quarter", "annual"] = "quarter",
) -> dict:
    """
    Screen a ticker universe for estimate momentum.

    Compares latest estimates against a lookback snapshot (`days` ago) and
    returns per-ticker deltas/direction. If `tickers` is omitted, screens the
    full stored universe.

    Args:
        tickers: Optional ticker list to screen. Example: ["AAPL", "MSFT"].
        days: Lookback window for baseline comparison (default: 30).
        direction: Filter direction:
            - "up": Positive estimate revisions only
            - "down": Negative estimate revisions only
            - "all": Return all directions (default)
        period: Estimate horizon:
            - "quarter": Quarterly estimates (default)
            - "annual": Annual estimates

    Returns:
        Ranked revision summary rows with EPS/revenue deltas.
    """
    if not _HAS_ESTIMATES:
        return {
            "status": "error",
            "error": "Estimate tools unavailable: install optional dependency with fmp-mcp[estimates].",
        }
    return _screen_estimate_revisions(
        tickers=tickers,
        days=days,
        direction=direction,
        period=period,
    )


@mcp.tool()
def get_economic_data(
    mode: Literal["indicator", "calendar"] = "indicator",
    indicator_name: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    country: Optional[str] = "US",
    format: Literal["full", "summary"] = "summary",
    limit: Optional[int] = None,
    output: Literal["inline", "file"] = "inline",
    use_cache: bool = True,
) -> dict:
    """
    Get economic indicators or upcoming economic events.

    Fetches macroeconomic data from FRED via FMP. Use indicator mode for
    time series data (GDP, CPI, unemployment, etc.) or calendar mode for
    upcoming/recent economic releases.

    Args:
        mode: Data type to fetch:
            - "indicator": Economic indicator time series (requires indicator_name)
            - "calendar": Upcoming economic events with forecasts and actuals
        indicator_name: Indicator to fetch (required for indicator mode).
            Available: GDP, realGDP, CPI, inflationRate, federalFunds,
            unemploymentRate, totalNonfarmPayroll, initialClaims,
            consumerSentiment, retailSales, durableGoods,
            industrialProductionTotalIndex, housingStarts, totalVehicleSales,
            smoothedUSRecessionProbabilities, 30YearFixedRateMortgageAverage,
            tradeBalanceGoodsAndServices.
        from_date: Start date in YYYY-MM-DD format (optional).
        to_date: End date in YYYY-MM-DD format (optional).
        country: Country filter for calendar mode (default: "US").
        format: Output format:
            - "summary": Latest value, trend, and key context
            - "full": Complete time series or event list
        limit: Optional cap for indicator mode (most recent N rows).
        output: Output mode for indicator full data: inline or file.
        use_cache: Use cached data when available (default: True).

    Returns:
        Economic data with status field ("success" or "error").

    Examples:
        "What's the latest GDP?" -> get_economic_data(indicator_name="GDP")
        "Show me CPI trend" -> get_economic_data(indicator_name="CPI")
        "What's the fed funds rate?" -> get_economic_data(indicator_name="federalFunds")
        "Upcoming economic events" -> get_economic_data(mode="calendar")
        "Economic calendar this week" -> get_economic_data(mode="calendar")
    """
    return _get_economic_data(
        mode=mode,
        indicator_name=indicator_name,
        from_date=from_date,
        to_date=to_date,
        country=country,
        format=format,
        limit=limit,
        output=output,
        use_cache=use_cache,
    )


@mcp.tool()
def get_sector_overview(
    date: Optional[str] = None,
    sector: Optional[str] = None,
    symbols: Optional[list[str]] = None,
    level: Literal["sector", "industry"] = "sector",
    format: Literal["full", "summary"] = "summary",
    use_cache: bool = True,
) -> dict:
    """
    Get sector or industry performance and valuation overview.

    Combines daily performance snapshot and P/E ratio snapshot into a
    unified heatmap view. Supports both sector-level and industry-level
    granularity.

    Args:
        date: Snapshot date in YYYY-MM-DD format (optional, defaults to latest).
        sector: Filter to one sector or industry (e.g., "Technology", "Energy",
            "Healthcare"). If not provided, returns all sectors/industries.
        symbols: Optional list of stock symbols for per-stock P/E comparison mode.
            Example: ["AAPL", "MSFT", "NVDA"]. Cannot be combined with `sector`.
        level: Granularity level:
            - "sector": GICS sector level (default, ~11 sectors)
            - "industry": More granular industry level
        format: Output format:
            - "summary": Sector heatmap with performance + valuation
            - "full": Complete raw data from all endpoints
        use_cache: Use cached data when available (default: True).

    Returns:
        Sector overview with status field ("success" or "error").

    Examples:
        "How are sectors performing?" -> get_sector_overview()
        "Technology sector overview" -> get_sector_overview(sector="Technology")
        "Which sectors are cheapest?" -> get_sector_overview()
        "Industry-level breakdown" -> get_sector_overview(level="industry")
        "Energy sector performance" -> get_sector_overview(sector="Energy")
        "Compare AAPL and MSFT vs sector P/E" -> get_sector_overview(symbols=["AAPL", "MSFT"])
        "Compare at industry level" -> get_sector_overview(symbols=["AAPL", "XOM"], level="industry")
    """
    return _get_sector_overview(
        date=date,
        sector=sector,
        symbols=symbols,
        level=level,
        format=format,
        use_cache=use_cache,
    )


@mcp.tool()
def get_market_context(
    include: Optional[list[str]] = None,
    format: Literal["full", "summary"] = "summary",
    use_cache: bool = True,
) -> dict:
    """
    Get a one-call market snapshot across indices, sectors, movers, and events.

    Useful for quick "what's happening in the market?" checks. Fetches major
    index moves, sector heatmap, top gainers/losers, most active names, and
    upcoming high-impact economic events.

    Args:
        include: Optional section filter. Available sections:
            - "indices": S&P 500, Dow Jones, Nasdaq, Russell 2000
            - "sectors": Sector performance heatmap
            - "gainers": Top daily gainers
            - "losers": Top daily losers
            - "actives": Most active by volume
            - "events": Upcoming high-impact economic events
            Default: all sections.
        format: Output format:
            - "summary": Curated top items (top 5 movers/events)
            - "full": Same normalized fields with no item limits
        use_cache: Use cached data when available (default: True).

    Returns:
        Market context with status field ("success" or "error"), section data,
        per-source fetch status, and warnings for any partial failures.

    Examples:
        "What's happening in the market?" -> get_market_context()
        "Show indices and gainers only" -> get_market_context(include=["indices", "gainers"])
        "Full market context" -> get_market_context(format="full")
    """
    return _get_market_context(
        include=include,
        format=format,
        use_cache=use_cache,
    )


@mcp.tool()
def get_institutional_ownership(
    symbol: str,
    year: Optional[int] = None,
    quarter: Optional[int] = None,
    limit: int = 20,
    format: Literal["full", "summary"] = "summary",
    use_cache: bool = True,
) -> dict:
    """
    Get institutional ownership analytics for a stock.

    Combines institutional holder-level data with aggregate ownership summary
    metrics to show which institutions are accumulating or reducing positions.

    Args:
        symbol: Stock symbol (e.g., "AAPL", "MSFT").
        year: Optional filing year filter.
        quarter: Optional filing quarter filter (1-4).
        limit: Max holder rows to return in summary mode (default: 20).
        format: Output format:
            - "summary": Normalized top holders and key ownership summary
            - "full": Raw endpoint payloads
        use_cache: Use cached data when available (default: True).

    Returns:
        Institutional ownership data with status field ("success" or "error").
    """
    return _get_institutional_ownership(
        symbol=symbol,
        year=year,
        quarter=quarter,
        limit=limit,
        format=format,
        use_cache=use_cache,
    )


@mcp.tool()
def get_insider_trades(
    symbol: str,
    limit: int = 20,
    format: Literal["full", "summary"] = "summary",
    use_cache: bool = True,
) -> dict:
    """
    Get insider buying/selling activity and summary statistics for a stock.

    Fetches recent insider transactions and aggregate insider trading stats.

    Args:
        symbol: Stock symbol (required, e.g., "AAPL").
        limit: Max trade rows to return in summary mode (default: 20).
        format: Output format:
            - "summary": Normalized recent trades and key stats
            - "full": Raw endpoint payloads
        use_cache: Use cached data when available (default: True).

    Returns:
        Insider trading data with status field ("success" or "error").
    """
    return _get_insider_trades(
        symbol=symbol,
        limit=limit,
        format=format,
        use_cache=use_cache,
    )


@mcp.tool()
def get_etf_holdings(
    symbol: str,
    include: Optional[list[str]] = None,
    limit: int = 25,
    format: Literal["full", "summary"] = "summary",
    output: Literal["inline", "file"] = "inline",
    use_cache: bool = True,
) -> dict:
    """
    Get ETF/fund composition across holdings, sector/country weights, and metadata.

    Args:
        symbol: ETF symbol (e.g., "SPY", "QQQ", "VTI").
        include: Optional section subset. Valid sections:
            "holdings", "sectors", "countries", "info", "exposure", "disclosure".
            Default: all sections.
        limit: Max rows in summary mode for holdings/exposure/disclosure (default: 25).
        format: Output format:
            - "summary": Normalized section outputs
            - "full": Raw endpoint payloads
        output: Output mode for full holdings section: inline or file.
        use_cache: Use cached data when available (default: True).

    Returns:
        ETF holdings/allocation data with status field ("success" or "error").
    """
    return _get_etf_holdings(
        symbol=symbol,
        include=include,
        limit=limit,
        format=format,
        output=output,
        use_cache=use_cache,
    )


@mcp.tool()
def get_news(
    symbols: Optional[str] = None,
    mode: Literal["stock", "general", "press"] = "stock",
    limit: int = 10,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    format: Literal["summary", "full"] = "summary",
    quality: Literal["all", "trusted", "wire", "journalism"] = "trusted",
) -> dict:
    """
    Fetch news articles for stocks or the broad market.

    Three modes: stock-specific news, general market news, and company press
    releases. Requires symbols for stock and press modes.
    Note: For portfolio-aware auto-fill, use get_portfolio_news on portfolio-mcp.

    Args:
        symbols: Comma-separated tickers (e.g., "AAPL,MSFT"). Required for
            "stock" and "press" modes.
        mode: News source:
            - "stock": Per-symbol news articles (default)
            - "general": Broad market news (symbols ignored)
            - "press": Official company press releases
        limit: Max articles to return, 1-50 (default: 10).
        from_date: Start date in YYYY-MM-DD format (optional).
        to_date: End date in YYYY-MM-DD format (optional).
        format: Output format:
            - "summary": Headline, date, source, snippet per article
            - "full": Complete article data
        quality: Source quality filter (default: "trusted"):
            - "trusted": Wire services + credible journalism (default)
            - "wire": Official press releases only (BusinessWire, PR Newswire, etc.)
            - "journalism": Credible financial journalism only (WSJ, Bloomberg, etc.)
            - "all": No filtering — includes all sources

    Returns:
        News data with status field ("success" or "error").

    Examples:
        "What's the news on AAPL?" -> get_news(symbols="AAPL")
        "Latest market news" -> get_news(mode="general")
        "TSLA press releases" -> get_news(symbols="TSLA", mode="press")
        "News for AAPL and MSFT" -> get_news(symbols="AAPL,MSFT")
        "Official AAPL news only" -> get_news(symbols="AAPL", quality="wire")
    """
    return _get_news(
        symbols=symbols,
        mode=mode,
        limit=limit,
        from_date=from_date,
        to_date=to_date,
        format=format,
        quality=quality,
    )


@mcp.tool()
def get_events_calendar(
    event_type: Literal["earnings", "dividends", "splits", "ipos", "all"] = "earnings",
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    symbols: Optional[str] = None,
    limit: Optional[int] = None,
    format: Literal["summary", "full"] = "summary",
) -> dict:
    """
    Fetch upcoming corporate events: earnings, dividends, splits, or IPOs.

    Can show market-wide calendars or filter to specific symbols.
    Useful for tracking upcoming catalysts and corporate actions.
    Note: For portfolio-aware auto-fill, use get_portfolio_events_calendar on portfolio-mcp.

    Args:
        event_type: Calendar type:
            - "earnings": Earnings dates with EPS estimates (default)
            - "dividends": Ex-dividend dates and amounts
            - "splits": Stock split dates
            - "ipos": Upcoming IPO dates
            - "all": All event types merged and sorted by date
        from_date: Start date in YYYY-MM-DD format (default: today).
        to_date: End date in YYYY-MM-DD format (default: today + 30 days).
            Max 90-day window.
        symbols: Comma-separated tickers to filter results (e.g., "AAPL,MSFT").
        limit: Maximum number of events to return (max: 500).
            If omitted, defaults to 20 for unfiltered event_type="all",
            otherwise 50. Applied after symbol filtering and date sorting.
        format: Output format:
            - "summary": Key event info (date, symbol, type-specific fields)
            - "full": Complete event data from FMP

    Returns:
        Calendar events with status field ("success" or "error").

    Examples:
        "Upcoming earnings?" -> get_events_calendar()
        "Dividend calendar for AAPL" -> get_events_calendar(event_type="dividends", symbols="AAPL")
        "All events this month" -> get_events_calendar(event_type="all")
        "Upcoming IPOs" -> get_events_calendar(event_type="ipos")
    """
    return _get_events_calendar(
        event_type=event_type,
        from_date=from_date,
        to_date=to_date,
        symbols=symbols,
        limit=limit,
        format=format,
    )


@mcp.tool()
def compare_peers(
    symbol: str,
    peers: Optional[str] = None,
    limit: int = 5,
    format: Literal["full", "summary"] = "summary",
) -> dict:
    """
    Compare a stock against its peers on key financial ratios.

    Fetches the peer group for a stock (companies in the same sector with
    similar market cap) and builds a side-by-side comparison of financial
    ratios including valuation, profitability, margins, and leverage.

    Args:
        symbol: Stock symbol to compare (e.g., "AAPL", "MSFT").
        peers: Optional comma-separated peer tickers (e.g., "MSFT,GOOGL,META").
            If not provided, peers are auto-discovered via FMP.
        limit: Maximum number of peers to include (default: 5, max: 10).
        format: Output format:
            - "summary": Comparison table with key metrics (P/E, P/B, P/S, ROE,
              ROA, gross/operating/net margin, debt/equity, current ratio,
              dividend yield, PEG ratio)
            - "full": All TTM ratios for each peer (60+ metrics)

    Returns:
        Peer comparison data with status field ("success" or "error").

    Examples:
        "Compare AAPL to its peers" -> compare_peers(symbol="AAPL")
        "How does MSFT stack up against peers?" -> compare_peers(symbol="MSFT")
        "Compare NVDA against AMD and INTC" -> compare_peers(symbol="NVDA", peers="AMD,INTC")
        "Show me GOOGL's peer group ratios" -> compare_peers(symbol="GOOGL", format="full")
    """
    return _compare_peers(
        symbol=symbol,
        peers=peers,
        limit=limit,
        format=format,
    )


@mcp.tool()
def get_technical_analysis(
    symbol: str,
    timeframe: Literal["1min", "5min", "15min", "30min", "1hour", "4hour", "1day"] = "1day",
    indicators: Optional[list[str]] = None,
    period_overrides: Optional[dict] = None,
    format: Literal["full", "summary"] = "summary",
    output: Literal["inline", "file"] = "inline",
    last_n: Optional[int] = None,
    use_cache: bool = True,
) -> dict:
    """
    Get composite technical analysis for a single stock or ETF.

    Fetches multiple technical indicators and provides trend direction,
    momentum signals, volatility analysis, and support/resistance levels
    with an overall buy/sell signal.

    Args:
        symbol: Stock or ETF symbol to analyze (e.g., "AAPL", "SPY").
        timeframe: Candle timeframe for analysis:
            - "1day": Daily (default, most common)
            - "1hour", "4hour": Intraday swing
            - "1min", "5min", "15min", "30min": Intraday scalping
        indicators: Optional subset of indicators to include. Options:
            "sma", "ema", "rsi", "adx", "williams", "macd", "bollinger".
            Default: all indicators.
        period_overrides: Override default period lengths. Example:
            {"sma_periods": [10, 50, 200], "rsi_period": 21}
        format: Output format:
            - "summary": Current signals and key values
            - "full": Signals plus complete time series data
        output: Output mode for full time series: inline or file.
        last_n: Optional cap for recent points per indicator in full mode.
        use_cache: Use cached indicator data when available (default: True).

    Returns:
        Technical analysis data with status field ("success" or "error").

    Examples:
        "Technical analysis for AAPL" -> get_technical_analysis(symbol="AAPL")
        "Is TSLA overbought?" -> get_technical_analysis(symbol="TSLA")
        "Show me MACD for NVDA" -> get_technical_analysis(symbol="NVDA", indicators=["macd"])
        "Hourly technicals for SPY" -> get_technical_analysis(symbol="SPY", timeframe="1hour")
        "Full technical data for MSFT" -> get_technical_analysis(symbol="MSFT", format="full")
    """
    return _get_technical_analysis(
        symbol=symbol,
        timeframe=timeframe,
        indicators=indicators,
        period_overrides=period_overrides,
        format=format,
        output=output,
        last_n=last_n,
        use_cache=use_cache,
    )


@mcp.tool()
def get_earnings_transcript(
    symbol: str,
    year: int,
    quarter: int,
    section: Literal["prepared_remarks", "qa", "all"] = "all",
    filter_speaker: Optional[str] = None,
    filter_role: Optional[Literal["CEO", "CFO", "COO", "CTO", "Analyst", "IR", "Operator"]] = None,
    format: Literal["full", "summary"] = "summary",
    max_words: Optional[int] = 3000,
    output: Literal["inline", "file"] = "inline",
) -> dict:
    """
    Parse and navigate an earnings call transcript.

    Splits the raw transcript into structured sections: prepared remarks
    and Q&A with per-speaker segments and grouped Q&A exchanges.

    IMPORTANT — default mode is summary (metadata only). This protects
    your context window. Full text is returned only when you explicitly
    request format="full".

    Recommended workflow:
    1. Call with default format="summary" to see the speaker list,
       word counts, and exchange count (costs ~1 KB of context)
    2. Identify the section or speaker you need
    3. Call again with format="full" and specific filters
       (section, filter_role, filter_speaker) to read only that content
    4. Each text field is capped at max_words (default 3000). Set
       max_words=None to remove the cap (use with caution).
    Note: If format="full" is used WITHOUT any filters (no section,
    filter_role, or filter_speaker), a bounded preview is returned
    instead (first 3 segments per section, 500 words each, 2 exchanges).
    Add at least one filter to get full content with max_words control.

    Args:
        symbol: Stock symbol (e.g., "AAPL", "MSFT", "NVDA").
        year: Fiscal year of the earnings call (e.g., 2024).
        quarter: Quarter 1-4.
        section: Which section to return:
            - "all": Both prepared remarks and Q&A (default)
            - "prepared_remarks": Management presentations only
            - "qa": Q&A session only
        filter_speaker: Filter to segments by this speaker (substring match,
            e.g., "Cook" matches "Tim Cook"). Case-insensitive.
        filter_role: Filter to segments by role:
            - "CEO", "CFO", "COO", "CTO": C-suite executives
            - "Analyst": Sell-side analysts asking questions
            - "IR": Investor Relations host
            - "Operator": Call operator
        format: Output format:
            - "summary": Metadata only — speaker list, word counts, exchange
              count. No text content. This is the DEFAULT. Use this first to
              scout the transcript before reading full text.
            - "full": Text content for all matching segments, truncated to
              max_words per field.
        output: Output mode:
            - "inline": Return content in MCP response (default).
            - "file": Write full untruncated markdown to disk and return
              metadata + absolute file_path.
        max_words: Maximum words per text field when format="full".
            Default 3000. Set to None for unlimited (use with caution —
            CEO prepared remarks can exceed 5K words). Ignored when
            format="summary" or output="file".

    Returns:
        dict with status and metadata. When format="full", also includes
        prepared_remarks, qa, and qa_exchanges with text content.

    Examples:
        # Step 1: Scout the transcript structure (default — summary mode)
        get_earnings_transcript(symbol="AAPL", year=2024, quarter=4)

        # Step 2: Drill into the CEO's prepared remarks
        get_earnings_transcript(symbol="AAPL", year=2024, quarter=4,
                                section="prepared_remarks", filter_role="CEO",
                                format="full")

        # Read the full Q&A session (truncated to 3000 words per field)
        get_earnings_transcript(symbol="AAPL", year=2024, quarter=4,
                                section="qa", format="full")

        # Find what the CFO said everywhere
        get_earnings_transcript(symbol="NVDA", year=2024, quarter=3,
                                filter_role="CFO", format="full")

        # Find a specific analyst's exchange
        get_earnings_transcript(symbol="MSFT", year=2024, quarter=4,
                                section="qa", filter_speaker="Nadella",
                                format="full")

        # Get full text without truncation (caution: may be very large)
        get_earnings_transcript(symbol="AAPL", year=2024, quarter=4,
                                section="prepared_remarks", filter_role="CEO",
                                format="full", max_words=None)
    """
    return _get_earnings_transcript(
        symbol=symbol,
        year=year,
        quarter=quarter,
        section=section,
        filter_speaker=filter_speaker,
        filter_role=filter_role,
        format=format,
        max_words=max_words,
        output=output,
    )


def _kill_previous_instance():
    """Kill any previous FMP MCP server instance spawned by the same parent session."""
    import os
    import signal
    import tempfile
    from pathlib import Path
    server_dir = Path(tempfile.gettempdir()) / "fmp-mcp"
    server_dir.mkdir(exist_ok=True)
    ppid = os.getppid()
    pid_file = server_dir / f".fmp_mcp_server_{ppid}.pid"
    if pid_file.exists():
        try:
            old_pid = int(pid_file.read_text().strip())
            if old_pid != os.getpid():
                os.kill(old_pid, signal.SIGTERM)
        except (ValueError, ProcessLookupError, PermissionError):
            pass
    pid_file.write_text(str(os.getpid()))
    # Clean up stale PID files from dead sessions
    for stale in server_dir.glob(".fmp_mcp_server_*.pid"):
        if stale == pid_file:
            continue
        try:
            session_pid = int(stale.stem.split("_")[-1])
            os.kill(session_pid, 0)  # check if parent session is alive
        except (ValueError, ProcessLookupError):
            stale.unlink(missing_ok=True)
        except PermissionError:
            pass  # process exists but owned by another user


def main():
    _kill_previous_instance()
    mcp.run()


if __name__ == "__main__":
    main()
