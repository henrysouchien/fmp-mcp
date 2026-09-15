# fmp-mcp

Financial intelligence for your AI agent — powered by live market data.

Give Claude (or any MCP-compatible AI) the ability to analyze stocks, screen markets, compare peers, read earnings calls, and track sector rotations — all grounded in real financial data from [Financial Modeling Prep](https://financialmodelingprep.com/).

This isn't a raw API wrapper. Each tool is purpose-built for a specific analytical task, composing multiple data sources into structured, analysis-ready outputs designed for AI consumption.

Tool surface below was verified against `fmp/server.py` on 2026-04-30. The repo build exposes 20 MCP tools; the two estimate-revision tools require the `estimates` extra plus database access for useful live results.

## What your AI can do

**Market Intelligence**
- `get_market_context` — One-call market snapshot: indices, sectors, gainers/losers, economic events
- `get_sector_overview` — Sector and industry performance with P/E valuations
- `get_news` — Stock-specific or broad market news
- `get_events_calendar` — Earnings, dividends, splits, and IPO calendars
- `get_economic_data` — Economic indicators and high-impact event tracking

**Fundamental Analysis**
- `fmp_fetch` — Direct access to 60+ financial data endpoints (income statements, balance sheets, cash flows, key metrics, and more)
- `fmp_market_cap_check` — Compare current market cap against the latest annual filing value
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

Optional local estimate storage (requires PostgreSQL; hosted MCP reads do not):

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

## Application integration

The installed `fmp-mcp` distribution owns the `fmp` import. It does not search
for a Risk checkout, load a checkout `.env`, or import application policy.
The source package lives at Risk's `fmp-mcp/`; install it into a development
interpreter with `uv pip install --python .venv/bin/python --no-deps -e ./fmp-mcp`
from the Risk root.

Applications must explicitly configure the client once at startup before any
network fetch. An unconfigured client refuses dispatch and names the required
`configure_client(...)` call. Applications can bind these callbacks:

- `fmp.client.configure_client(budget_guard=..., event_observer=...)`.
  The guard accepts `fn`, `args`, `kwargs` plus provider/operation/budget
  metadata. The observer receives `(event, endpoint, **details)`, where event
  is `success` (`duration_ms`, `status`), `rate_limit`, `error` (`error`), or
  `plan_limit` (`error`). Explicit `configure_client()` selects the public
  default: no budget guard, direct requests, and native Python logging for
  rate limits and errors. The public `fmp-mcp` / `python -m fmp.server`
  entrypoint selects this policy before serving tools.
- `fmp.tools.peers.configure_peer_tools(metric_resolver=..., peer_discovery=...,
  spot_fx_rate=...)`. Metric resolvers return the existing structural
  `peers`/`source`/`to_dict()` result; discovery returns ticker strings.
  Expected policy failures raise `PeerSelectionError`. Standalone discovery
  uses FMP `stock_peers`; metric-context selection without application policy
  asks for explicit peers. Standalone non-USD conversion reads the latest
  daily close of FMP's `<currency>USD` pair. The optional FX callback returns
  a currency-to-USD rate.

Risk process entrypoints call `fmp_runtime.configure()` after environment
hydration to bind all five callbacks; logging, billing, peer selection, and
exchange-mapping policy remain application code. `bootstrap_env.bootstrap()`
only hydrates environment and never imports FMP or application policy.
`scripts/run_fmp_server.py` owns Risk credential hydration and policy setup
before invoking the installed `fmp.server.run()` (which preserves the
caller's policy). Risk-only wrappers live in `utils/fmp_compat.py` and
`utils/fmp_fx.py`, and the local collector lives in `scripts/snapshot_estimates.py`.

The source retains the Hank-only `transcript_kpi_fetcher.py` and
`manifest_source_dispatcher.py`; public distribution sync excludes them.
Their application consumers require the separately installed named industry
and value-semantics libraries. Sync copies source bytes without vendoring or
rewriting imports.

## Run

```bash
fmp-mcp
```

Or register the installed package with Claude Code from any directory:

```bash
claude mcp add fmp-mcp --scope user \
  -- python3 -m fmp.server
```

You can also use a generic MCP config:

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

## Related Docs

- `docs/reference/FMP_ENDPOINTS.md` — registered endpoint catalog
- `docs/reference/MCP_SERVERS.md` — server registration and troubleshooting
