# FMP Data Abstraction Layer

A unified interface for Financial Modeling Prep (FMP) API data access with:
- **Discoverable endpoints** with full metadata
- **Disk caching** (Parquet + Zstandard compression)
- **Structured error handling**
- **Backward-compatible wrappers**

## Documentation

| File | Purpose |
|------|---------|
| `README.md` | This file - full documentation |
| `AGENT_PROMPTING_GUIDE.md` | Quick reference for prompting Claude agents |
| `API_REFERENCE.json` | Machine-readable schema with all endpoints, parameters, and response columns |
| `server.py` | MCP server for direct Claude tool access (`python3 -m fmp.server`) |

## Quick Start

```python
from fmp import FMPClient

fmp = FMPClient()

# Fetch data
prices = fmp.fetch("historical_price_adjusted", symbol="AAPL")
income = fmp.fetch("income_statement", symbol="AAPL", period="quarter", limit=4)

# Discover endpoints
fmp.list_endpoints()                    # All endpoints
fmp.list_endpoints(category="analyst")  # Filter by category
fmp.describe("income_statement")        # Full documentation
```

## Available Endpoints

| Category | Endpoint | Description |
|----------|----------|-------------|
| **analyst** | `analyst_estimates` | Analyst estimates (EPS, revenue forecasts) |
| **analyst** | `price_target` | Analyst price target summary (high, low, median, consensus) |
| **analyst** | `price_target_consensus` | Analyst price target consensus |
| **calendar** | `earnings_calendar` | Upcoming and recent earnings dates with estimates |
| **calendar** | `dividends_calendar` | Upcoming ex-dividend dates and amounts |
| **calendar** | `splits_calendar` | Upcoming stock split dates |
| **calendar** | `ipos_calendar` | Upcoming IPO dates and pricing |
| **dividends** | `dividends` | Dividend history (payment dates, amounts, frequency) |
| **etf** | `etf_holdings` | ETF holdings with constituent weights |
| **etf** | `etf_holdings_v3` | ETF holdings (v3 legacy -- broader plan access) |
| **etf** | `etf_info` | ETF metadata and fund-level details |
| **etf** | `etf_country_weightings` | ETF country allocation weightings |
| **etf** | `etf_sector_weightings` | ETF sector allocation weightings |
| **etf** | `etf_asset_exposure` | ETF asset exposure breakdown |
| **etf** | `etf_disclosure` | Latest fund disclosure holders |
| **filings** | `sec_filings` | SEC filing metadata with links (10-K, 10-Q, 8-K, S-1) |
| **fundamentals** | `income_statement` | Revenue, net income, EPS, margins |
| **fundamentals** | `balance_sheet` | Assets, liabilities, equity |
| **fundamentals** | `cash_flow` | Operating, investing, financing cash flows |
| **fundamentals** | `key_metrics` | P/E, P/B, ROE, debt ratios |
| **fundamentals** | `ratios_ttm` | Trailing twelve month financial ratios |
| **insider** | `insider_trades_search` | Search insider trades by symbol |
| **insider** | `insider_trade_statistics` | Insider trading statistics for a symbol |
| **insider** | `insider_transaction_types` | Reference list of insider transaction types |
| **institutional** | `institutional_holders` | Institutional holders with share change analytics |
| **institutional** | `institutional_positions_summary` | Aggregate institutional ownership summary |
| **institutional** | `institutional_holder_performance` | Performance summary for an institutional holder (CIK) |
| **institutional** | `institutional_industry_breakdown` | Industry-level breakdown for an institutional holder |
| **institutional** | `institutional_filings_dates` | Available 13F filing dates for an institutional holder |
| **institutional** | `institutional_holders_v3` | Institutional holders (v3 legacy -- broader plan access) |
| **macro** | `economic_indicators` | Economic indicator time series (GDP, CPI, unemployment, etc.) |
| **macro** | `economic_calendar` | Upcoming/recent economic events with prior/forecast/actual values |
| **market_movers** | `biggest_gainers` | Top gaining stocks by daily percentage change |
| **market_movers** | `biggest_losers` | Top losing stocks by daily percentage change |
| **market_movers** | `most_actives` | Most actively traded stocks by volume |
| **news** | `news_stock` | Stock-specific news articles |
| **news** | `news_general` | Latest general market news |
| **news** | `news_press_releases` | Official company press releases |
| **prices** | `historical_price_eod` | Daily OHLCV prices |
| **prices** | `historical_price_adjusted` | Dividend-adjusted prices (for total return) |
| **quotes** | `batch_index_quotes` | Batch quotes for market indices |
| **screening** | `company_screener` | Screen stocks by market cap, sector, beta, price, dividend, etc. |
| **screening** | `stock_peers` | Get peer companies for a stock (same sector, similar market cap) |
| **sector** | `sector_performance_snapshot` | Daily sector percentage change snapshot |
| **sector** | `industry_performance_snapshot` | Daily industry percentage change snapshot |
| **sector** | `sector_pe_snapshot` | Sector aggregate P/E ratio snapshot |
| **sector** | `industry_pe_snapshot` | Industry aggregate P/E ratio snapshot |
| **technical** | `ta_sma` | Simple Moving Average |
| **technical** | `ta_ema` | Exponential Moving Average |
| **technical** | `ta_rsi` | Relative Strength Index (0-100) |
| **technical** | `ta_adx` | Average Directional Index (trend strength) |
| **technical** | `ta_williams` | Williams %R (-100 to 0) |
| **technical** | `ta_stddev` | Standard Deviation (volatility) |
| **transcripts** | `earnings_transcript` | Full earnings call transcript text |
| **treasury** | `treasury_rates` | US Treasury rates (all maturities) |
| **search** | `search` | Search companies by name/ticker |
| **search** | `profile` | Company profile (sector, industry, description) |

## Endpoint Parameters

### Common Parameters

Most endpoints accept:
- `symbol` (required): Stock ticker (e.g., "AAPL", "MSFT")
- `limit`: Number of records to return (default: 10)
- `period`: "annual" or "quarter" (for fundamentals)

### Date Parameters

For price/treasury endpoints:
- `from`: Start date as "YYYY-MM-DD" string
- `to`: End date as "YYYY-MM-DD" string

**Note:** Use keyword syntax for `from`/`to` since `from` is a Python reserved word:
```python
fmp.fetch("treasury_rates", **{"from": "2023-01-01", "to": "2024-12-31"})
```

The client also accepts friendlier aliases: `from_date`, `to_date`, `start_date`, `end_date` -- these are mapped to `from`/`to` automatically.

## Response Format

All `fetch()` calls return a pandas DataFrame:

```python
df = fmp.fetch("income_statement", symbol="AAPL", limit=3)
print(df.columns.tolist())
# ['date', 'symbol', 'revenue', 'netIncome', 'eps', ...]
```

For raw JSON (no caching, no DataFrame conversion):

```python
raw = fmp.fetch_raw("income_statement", symbol="AAPL", limit=3)
# Returns list[dict] or dict
```

## Caching

Disk-based caching with per-endpoint refresh strategies:

| Strategy | Behavior | Use Case |
|----------|----------|----------|
| `HASH_ONLY` | Cache by param hash only | Immutable historical data (fundamentals) |
| `MONTHLY` | Add month token to key | Data updated monthly (dividends) |
| `TTL` | Expire after N hours | Frequently changing data (analyst) |

**Staleness protection**: For `HASH_ONLY` endpoints with date params (prices, treasury), if no `to` date is specified, a monthly token is added to prevent stale "latest data" cache hits.

**Per-endpoint cache disabling**: Some endpoints (e.g., `search`) have `cache_enabled=False` and always hit the API.

Bypass cache with:
```python
df = fmp.fetch("income_statement", symbol="AAPL", use_cache=False)
```

## Rate Limiting

`FMPClient` includes a built-in sliding-window rate limiter (default: 700 calls/min, 50-call buffer under the 750/min plan limit). All API calls go through `_make_request()` which enforces this automatically — no manual `time.sleep()` needed.

If a 429 (rate limited) response is received despite the proactive limiter (e.g., due to cross-process API key contention), the client retries up to 3 times with 30-second backoff before raising `FMPRateLimitError`.

```python
# Custom rate limit (e.g., for shared API keys)
fmp = FMPClient(max_calls_per_minute=500)
```

## Error Handling

```python
from fmp import FMPClient
from fmp.exceptions import (
    FMPEndpointError,
    FMPEmptyResponseError,
    FMPRateLimitError,
    FMPValidationError,
    FMPAuthenticationError,
    FMPAPIError,
)

try:
    df = fmp.fetch("income_statement", symbol="INVALID")
except FMPEmptyResponseError:
    print("No data found for symbol")
except FMPRateLimitError:
    print("Rate limited - wait and retry")
except FMPValidationError:
    print("Invalid parameters")
except FMPAuthenticationError:
    print("Missing or invalid FMP_API_KEY")
```

All exceptions inherit from `FMPError`.

## Adding New Endpoints

Single registration in `fmp/registry.py`:

```python
from fmp.registry import CacheRefresh

register_endpoint(FMPEndpoint(
    name="analyst_recommendations",
    path="/analyst-recommendations",           # Path without base URL prefix
    description="Analyst buy/sell/hold recommendations",
    fmp_docs_url="https://site.financialmodelingprep.com/developer/docs#...",
    category="analyst",
    api_version="stable",                       # "stable" or "v3" - determines base URL
    params=[EndpointParam("symbol", ParamType.STRING, required=True)],
    cache_refresh=CacheRefresh.TTL,             # TTL, MONTHLY, or HASH_ONLY
    cache_ttl_hours=24,                         # Required when cache_refresh=TTL
    response_path=None,                         # Dot-path for nested data (e.g., "data.items")
    response_transform=None,                    # Optional: callable(df) -> df for shaping
))
```

Immediately usable: `fmp.fetch("analyst_recommendations", symbol="AAPL")`

**Note:** The FMP MCP server (`fmp/server.py`, invoked via `python3 -m fmp.server`) wraps this package for direct Claude access. When adding new endpoints or modifying the client API, ensure the MCP tools remain compatible. The MCP server currently exposes 19 tools:

| MCP Tool | Description |
|----------|-------------|
| `fmp_fetch` | Fetch data from any registered endpoint |
| `fmp_search` | Search for companies by name/ticker |
| `fmp_profile` | Get company profile details |
| `fmp_list_endpoints` | List available endpoints |
| `fmp_describe` | Get endpoint parameter documentation |
| `screen_stocks` | Screen stocks by fundamental criteria |
| `get_estimate_revisions` | Estimate revision history for a ticker |
| `screen_estimate_revisions` | Screen for estimate momentum across tickers |
| `compare_peers` | Compare a stock against its peers on financial ratios |
| `get_technical_analysis` | Composite technical analysis (trend, momentum, volatility) |
| `get_economic_data` | Economic indicators and calendar events |
| `get_sector_overview` | Sector/industry performance and P/E valuation |
| `get_market_context` | One-call market snapshot (indices, sectors, movers, events) |
| `get_institutional_ownership` | Institutional holder analytics and ownership summary |
| `get_insider_trades` | Insider trade flow and statistics |
| `get_etf_holdings` | ETF holdings, sector/country allocation, metadata |
| `get_news` | News articles (stock-specific, general, press releases) |
| `get_events_calendar` | Corporate event calendars (earnings, dividends, splits, IPOs) |
| `get_earnings_transcript` | Parse and navigate earnings call transcripts |

---

# Prompting Agents to Use This Package

This section documents how to effectively prompt Claude agents to use the FMP package for financial analysis tasks.

> **For AI/Agents:** See `API_REFERENCE.json` for a machine-readable schema with all endpoint parameters and response columns. See `AGENT_PROMPTING_GUIDE.md` for a condensed quick reference.

## Basic Prompt Template

```
Use the FMP package at `fmp/` to [describe analysis task].

```python
from fmp import FMPClient
fmp = FMPClient()
```

Use the `[endpoint_name]` endpoint to fetch [data description] for [symbols].

[Specific parameters if needed]

Analyze:
1. [First question]
2. [Second question]
3. [Third question]

Write and execute a Python script.
```

## Key Elements for Effective Prompts

1. **Always include the import snippet** - Agents need to know how to initialize
2. **Specify the endpoint name** - Use exact names from the table above
3. **List the symbols/tickers** - Be explicit about what to fetch
4. **Mention relevant columns** - Helps agent know what data to expect
5. **Ask specific questions** - Structured questions get structured answers

## Example Prompts by Category

### Price Analysis

```
Use the FMP package at `fmp/` to analyze price performance.

```python
from fmp import FMPClient
fmp = FMPClient()
```

Use the `historical_price_adjusted` endpoint to fetch dividend-adjusted prices
for NVDA, AMD, and INTC from 2023-01-01 to 2024-12-31.

Calculate and compare:
1. Total return for each stock over the period
2. Max drawdown for each
3. Which performed best?

Write and execute a Python script. The endpoint returns columns like
'date', 'adjClose', etc.
```

### Fundamental Analysis

```
Use the FMP package at `fmp/` to analyze free cash flow generation.

```python
from fmp import FMPClient
fmp = FMPClient()
```

Use the `cash_flow` endpoint to fetch cash flow statements for GOOGL, META,
and AMZN (last 3 years, annual).

Analyze:
1. Free cash flow trend for each company
2. FCF margin (FCF / Revenue - use income_statement for revenue)
3. Capital expenditure intensity
4. Which company generates the most FCF per dollar of revenue?

Write and execute a Python script.
```

### Treasury/Rates Analysis

```
Use the FMP package at `fmp/` to analyze Treasury yield curve changes.

```python
from fmp import FMPClient
fmp = FMPClient()
```

Use the `treasury_rates` endpoint to fetch Treasury rates from 2023-01-01
to 2024-12-31.

The data includes columns: 'date', 'month1', 'month3', 'month6', 'year1',
'year2', 'year5', 'year10', 'year30'.

Analyze:
1. How did the 2Y-10Y spread change? (Was the curve inverted?)
2. What was the peak short-term rate (month3)?
3. Show yield curve shape at start vs end of period

Write and execute a Python script.
```

### Company Search

```
Use the FMP package at `fmp/` to search for companies.

```python
from fmp import FMPClient
fmp = FMPClient()
```

1. Use the `search` endpoint to search for "artificial intelligence"
   companies (query="artificial intelligence", limit=10)

2. For the top 3 results, use the `profile` endpoint to get company details

3. Summarize: company name, sector, industry, market cap, description

Write and execute a Python script.
```

### Dividend Analysis

```
Use the FMP package at `fmp/` to analyze dividend history.

```python
from fmp import FMPClient
fmp = FMPClient()
```

Use the `dividends` endpoint to fetch dividend history for JNJ, PG, and KO.

Analyze:
1. Current dividend amount for each
2. Payment frequency (monthly vs quarterly)
3. Has the dividend been growing?
4. Which has the highest current yield? (use `profile` for price)

Write and execute a Python script.
```

### Analyst Expectations

```
Use the FMP package at `fmp/` to analyze analyst expectations.

```python
from fmp import FMPClient
fmp = FMPClient()
```

Use the `analyst_estimates` endpoint to fetch estimates for TSLA
(both annual and quarterly, limit=5 each).

Also fetch `price_target` to see analyst price expectations.

Analyze:
1. Expected EPS trajectory over next few years
2. Expected revenue growth
3. Current price target consensus

Write and execute a Python script.
```

### SEC Filings Research

```
Use the FMP package at `fmp/` to research a company's SEC filings.

```python
from fmp import FMPClient
fmp = FMPClient()
```

Use the `sec_filings` endpoint to list filings for [TICKER].
Parameters: symbol="AAPL", type="10-K" (optional filter), limit=10

The response includes `finalLink` column with direct links to SEC.gov filings.

Analyze:
1. What are the most recent filings?
2. When was the last 10-K (annual report) filed?
3. Any recent 8-Ks (material events)?

Note: This endpoint returns metadata and links. To read the actual filing content,
fetch the URL from `finalLink` column. SEC.gov requires a User-Agent header:

```python
import requests
headers = {'User-Agent': 'Your Name (contact@example.com)'}
resp = requests.get(filing_url, headers=headers)
```

Write and execute a Python script.
```

### Earnings Call Analysis

```
Use the FMP package at `fmp/` to analyze a company's earnings call.

```python
from fmp import FMPClient
fmp = FMPClient()
```

Use the `earnings_transcript` endpoint to fetch the Q4 2024 earnings call for AAPL.
Parameters: symbol="AAPL", year=2024, quarter=4

The response includes a `content` column with the full transcript text.

Analyze:
1. What were the key themes management discussed?
2. Any forward guidance or outlook mentioned?
3. What questions did analysts focus on?
4. Any risks or challenges highlighted?

Write and execute a Python script.
```

## Common Column Names by Endpoint

### income_statement
`date`, `symbol`, `revenue`, `netIncome`, `eps`, `grossProfit`, `operatingIncome`, `ebitda`

### balance_sheet
`date`, `symbol`, `totalAssets`, `totalLiabilities`, `totalEquity`, `cash`, `totalDebt`, `currentAssets`, `currentLiabilities`

### cash_flow
`date`, `symbol`, `operatingCashFlow`, `capitalExpenditure`, `freeCashFlow`, `dividendsPaid`

### key_metrics
`date`, `symbol`, `marketCap`, `returnOnEquity`, `returnOnAssets`, `currentRatio`, `debtToEquity`, `evToEBITDA`

### historical_price_adjusted
`date`, `open`, `high`, `low`, `close`, `adjClose`, `volume`

### treasury_rates
`date`, `month1`, `month3`, `month6`, `year1`, `year2`, `year5`, `year10`, `year30`

### dividends
`date`, `adjDividend`, `dividend`, `yield`, `frequency`

### profile
`symbol`, `companyName`, `sector`, `industry`, `marketCap`, `price`, `description`, `currency`

## Estimate Revision Tracking

The FMP layer includes a local estimate store that snapshots analyst consensus estimates over time, enabling revision tracking and estimate momentum screening.

### How It Works

A monthly cron job (`fmp/scripts/snapshot_estimates.py`) fetches consensus estimates for all FMP-covered tickers and stores them in a dedicated Postgres database (`fmp_data_db`, configurable via `FMP_DATA_DATABASE_URL`). Each snapshot is immutable — over time, the database accumulates a history of how the Street's estimates have moved.

### Usage

```python
from fmp.estimate_store import EstimateStore

store = EstimateStore(read_only=True)

# Latest consensus for a ticker
latest = store.get_latest("AAPL", period="quarter")

# Revision history for a specific fiscal period
revisions = store.get_revisions("AAPL", fiscal_date="2026-06-28")

# Screen for estimate momentum (latest vs 30 days ago)
momentum = store.get_revision_summary(["AAPL", "NVDA", "MSFT"], days=30)

# Check snapshot freshness
freshness = store.get_freshness(["AAPL", "NVDA"])

# List all tracked tickers
tickers = store.list_tickers()
```

### MCP Tools

Two query tools are available via the `fmp-mcp` server:

| Tool | Description |
|------|-------------|
| `get_estimate_revisions` | Revision history for a single ticker (snapshot-by-snapshot deltas) |
| `screen_estimate_revisions` | Screen across tickers for estimate momentum (up/down/all) |

### Collection Script

```bash
# Full universe collection (monthly cron) — uses bulk∩screener intersection by default
python3 fmp/scripts/snapshot_estimates.py

# Use screener-only universe (legacy, no bulk filtering)
python3 fmp/scripts/snapshot_estimates.py --universe-source screener

# Test with specific tickers
python3 fmp/scripts/snapshot_estimates.py --tickers AAPL,NVDA,MSFT

# Force re-snapshot (bypass freshness check)
python3 fmp/scripts/snapshot_estimates.py --tickers AAPL --force

# Override DB connection for one run
python3 fmp/scripts/snapshot_estimates.py --database-url postgresql://postgres@localhost:5432/fmp_data_db

# Key flags
#   --universe-source bulk  Universe method: bulk (screener∩earnings-surprises-bulk) or screener
#   --bulk-years 2          Years of bulk data to union (default: 2)
#   --delay-ms 100          Delay between API calls in ms (default: 100)
#   --freshness-days 28     Skip tickers snapshotted within N days (default: 28)
#   --force                 Bypass freshness check
#   --no-resume             Don't resume a prior interrupted run
#   --universe-limit 100    Limit ticker universe (for testing)
```

### Storage

- **Database:** `fmp_data_db` on Postgres (`snapshot_runs` + `estimate_snapshots` + `collection_failures`)
- **Connection:** `FMP_DATA_DATABASE_URL` (default: `postgresql://postgres@localhost:5432/fmp_data_db`)
- **Growth:** ~60K rows/run × 12 runs/year = ~720K rows/year, ~200-300MB/year of storage
- **Design:** Insert-only, immutable snapshots. UNIQUE constraint on `(ticker, fiscal_date, period, snapshot_date)` prevents duplicates.
- **Failure tracking:** `collection_failures` table records per-ticker errors with structured error types (`no_income_statement`, `no_estimates`, `api_error`, `unknown`). Query with `store.get_failure_summary(min_runs=2)` to find persistently failing tickers.

### Resource Profile

Based on first full production run (Feb 2026):

| Metric | Value |
|---|---|
| Universe size (bulk intersection) | ~4,927 tickers |
| Tickers with data | 4,880 (99.0%) |
| API calls per run | ~14,800 (3 per ticker: income_statement + 2 estimate periods) |
| FMP bandwidth per run | Negligible (small JSON responses) |
| Run duration | ~44 min (rate limiter at 700 calls/min) |
| Rows inserted per run | ~59,500 |
| Failures per run | ~6 no_income_statement, ~136 no_estimates (no forward periods) |
| Storage per run | ~15-20MB |
| Storage per year (12 runs) | ~200-300MB |
| Postgres capacity | Easily handles 5+ years of history (millions of rows) |

---

## Tips for Better Agent Results

1. **Be specific about date ranges** - Include explicit from/to dates
2. **Mention column names** - Reduces agent trial-and-error
3. **Ask for comparisons** - "Which is best?" forces ranking
4. **Request formatted output** - "Create a summary table" gets cleaner results
5. **Combine endpoints** - Many analyses need 2-3 endpoints together
