# fmp-mcp

Financial intelligence for your AI agent — powered by live market data.

Give Claude (or any MCP-compatible AI) the ability to analyze stocks, screen markets, compare peers, read earnings calls, and track sector rotations — all grounded in real financial data from [Financial Modeling Prep](https://financialmodelingprep.com/).

This isn't a raw API wrapper. Each tool is purpose-built for a specific analytical task, composing multiple data sources into structured, analysis-ready outputs designed for AI consumption.

## What your AI can do

**Market Intelligence**
- `get_market_context` — One-call market snapshot: indices, sectors, gainers/losers, economic events
- `get_sector_overview` — Sector and industry performance with P/E valuations
- `get_news` — Stock-specific or broad market news
- `get_events_calendar` — Earnings, dividends, splits, and IPO calendars
- `get_economic_data` — Economic indicators and high-impact event tracking

**Fundamental Analysis**
- `fmp_fetch` — Direct access to 60+ financial data endpoints (income statements, balance sheets, cash flows, key metrics, and more)
- `compare_peers` — Side-by-side peer comparison across 12 financial ratios
- `get_earnings_transcript` — Parsed earnings calls with speaker attribution and Q&A sections

**Stock Screening & Discovery**
- `screen_stocks` — Screen by sector, market cap, beta, dividend, volume, and more
- `get_institutional_ownership` — Institutional holder analytics and ownership trends
- `get_insider_trades` — Insider transaction flow and statistics
- `get_etf_holdings` — ETF/fund holdings, sector and country allocation

**Technical Analysis**
- `get_technical_analysis` — Composite signals from 7 indicators (SMA, EMA, RSI, MACD, Bollinger, ADX, Williams %R) with buy/sell scoring

**Analyst Sentiment**
- `get_estimate_revisions` — Historical EPS/revenue estimate revision trends
- `screen_estimate_revisions` — Screen for estimate momentum across a universe of stocks

**Data Discovery**
- `fmp_list_endpoints` — Browse all 60+ available data endpoints by category
- `fmp_describe` — Get parameter documentation for any endpoint
- `fmp_search` — Search for companies by name or ticker
- `fmp_profile` — Company profile with sector, industry, and key stats

## Install

```bash
pip install fmp-mcp
```

Optional estimate-revision tools (requires PostgreSQL):

```bash
pip install "fmp-mcp[estimates]"
```

## Configuration

Set your API key:

```bash
export FMP_API_KEY="your_key"
```

Optional settings:

- `FMP_CACHE_DIR` — Custom cache directory (default: `~/.cache/fmp-mcp/`)
- `FMP_CACHE_MAXSIZE` — Max in-memory cache entries (default: 200)

## Run

```bash
fmp-mcp
```

Or use with Claude Code:

```json
{
  "mcpServers": {
    "fmp-mcp": {
      "type": "stdio",
      "command": "uvx",
      "args": ["fmp-mcp"],
      "env": { "FMP_API_KEY": "your_key" }
    }
  }
}
```

## How it's different

| | Raw API wrapper | fmp-mcp |
|---|---|---|
| **Approach** | Expose every endpoint 1:1 | Purpose-built analytical tools |
| **Output** | Raw JSON, dozens of fields | Structured, summarized, analysis-ready |
| **Composition** | One API call per tool | Multiple sources stitched together |
| **AI-optimized** | Generic descriptions | Tool descriptions and schemas designed for LLM tool selection |
| **Caching** | None | Per-endpoint disk caching with configurable refresh strategies |

## Requirements

- Python 3.11+
- FMP API key ([get one here](https://financialmodelingprep.com/developer/docs/))
