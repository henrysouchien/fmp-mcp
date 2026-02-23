# FMP Agent Prompting Quick Reference

Use this guide when tasking Claude agents to perform financial analysis using the FMP package.

> **Full API Reference:** See `fmp/API_REFERENCE.json` for complete endpoint schemas and response columns.

## Basic Usage

```python
from fmp import FMPClient
from fmp.exceptions import FMPEmptyResponseError

fmp = FMPClient()

# All endpoints use fmp.fetch() - returns a pandas DataFrame
df = fmp.fetch("income_statement", symbol="AAPL", period="annual", limit=3)

# Check for empty results
if not df.empty:
    for _, row in df.iterrows():
        print(row['revenue'])

# Handle endpoints that may return no data (e.g., dividends for non-payers)
try:
    dividends = fmp.fetch("dividends", symbol="GOOG")
except FMPEmptyResponseError:
    print("No dividend history")
```

## Prompt Template

```
Use the FMP package at `fmp/` to [task].

```python
from fmp import FMPClient
from fmp.exceptions import FMPEmptyResponseError
fmp = FMPClient()
```

Use `fmp.fetch("[endpoint]", symbol="[SYMBOL]", ...)` to fetch [what].

Analyze:
1. [Question 1]
2. [Question 2]

Write and execute a Python script.
```

## Endpoint Quick Reference

| Endpoint | Use For | Key Params |
|----------|---------|------------|
| `historical_price_adjusted` | Stock returns, drawdowns | `symbol`, `from`, `to` |
| `historical_price_eod` | Raw OHLCV prices | `symbol`, `from`, `to` |
| `treasury_rates` | Yield curve, rates | `from`, `to` |
| `income_statement` | Revenue, earnings, margins | `symbol`, `period`, `limit` |
| `balance_sheet` | Assets, debt, equity | `symbol`, `period`, `limit` |
| `cash_flow` | FCF, capex, dividends paid | `symbol`, `period`, `limit` |
| `key_metrics` | ROE, P/E, ratios | `symbol`, `period`, `limit` |
| `dividends` | Dividend history | `symbol` |
| `analyst_estimates` | EPS/revenue forecasts | `symbol`, `period`, `limit` |
| `price_target` | Analyst price targets | `symbol` |
| `earnings_transcript` | Earnings call transcript text | `symbol`, `year`, `quarter` |
| `sec_filings` | SEC filing metadata with links | `symbol`, `type`, `limit` |
| `search` | Find companies | `query`, `limit` |
| `profile` | Company info | `symbol` |

## Ready-to-Use Prompts

### Stock Performance Comparison
```
Use the FMP package at `fmp/` to compare stock performance.

```python
from fmp import FMPClient
fmp = FMPClient()
# Example: fmp.fetch("historical_price_adjusted", symbol="AAPL", **{"from": "2024-01-01", "to": "2024-12-31"})
```

Use `historical_price_adjusted` to fetch prices for [TICKER1], [TICKER2], [TICKER3]
from [START_DATE] to [END_DATE].

Calculate: total return, max drawdown, and rank by performance.

Write and execute a Python script.
```

### Company Financial Health
```
Use the FMP package at `fmp/` to analyze [COMPANY] financial health.

```python
from fmp import FMPClient
fmp = FMPClient()
# Example: fmp.fetch("income_statement", symbol="AAPL", period="annual", limit=3)
```

Fetch `income_statement`, `balance_sheet`, and `key_metrics` for [TICKER] (last 3 years).
Use `profile` first to confirm you have the correct company.

Summarize: revenue trend, profitability (margins, ROE), and balance sheet strength.

Write and execute a Python script.
```

### Dividend Analysis
```
Use the FMP package at `fmp/` to compare dividends.

```python
from fmp import FMPClient
from fmp.exceptions import FMPEmptyResponseError
fmp = FMPClient()
# Note: dividends endpoint raises FMPEmptyResponseError for non-dividend payers
```

Use `dividends` endpoint for [TICKER1], [TICKER2], [TICKER3].
Handle FMPEmptyResponseError for companies that don't pay dividends.

Analyze: payment frequency, recent amounts, dividend growth, and current yield.

Write and execute a Python script.
```

### Analyst Expectations
```
Use the FMP package at `fmp/` to analyze analyst expectations for [TICKER].

```python
from fmp import FMPClient
fmp = FMPClient()
```

Fetch `analyst_estimates` (annual, limit=5) and `price_target`.

Summarize: EPS trajectory, revenue growth expectations, and price target consensus.

Write and execute a Python script.
```

### Sector/Industry Comparison
```
Use the FMP package at `fmp/` to compare companies in [SECTOR].

```python
from fmp import FMPClient
fmp = FMPClient()
```

For [TICKER1], [TICKER2], [TICKER3], fetch `key_metrics` and `income_statement`.

Compare: margins, ROE, growth rates, and valuation multiples.

Write and execute a Python script.
```

### Treasury Yield Analysis
```
Use the FMP package at `fmp/` to analyze Treasury yields.

```python
from fmp import FMPClient
fmp = FMPClient()
```

Use `treasury_rates` from [START_DATE] to [END_DATE].

Columns: month1, month3, month6, year1, year2, year5, year10, year30.

Analyze: yield curve shape, inversions, rate changes over period.

Write and execute a Python script.
```

## Common Column Names

**profile:** `companyName`, `sector`, `industry`, `currency`, `description`

**income_statement:** `date`, `calendarYear`, `revenue`, `netIncome`, `eps`, `grossProfit`, `operatingIncome`

**balance_sheet:** `date`, `totalAssets`, `totalStockholdersEquity`, `totalDebt`, `cashAndCashEquivalents`

**cash_flow:** `date`, `operatingCashFlow`, `freeCashFlow`, `capitalExpenditure`, `stockBasedCompensation`, `dividendsPaid`

**key_metrics:** `date`, `marketCap`, `enterpriseValue`, `peRatio`, `roe`, `roic`, `debtToEquity`, `evToSales`, `dividendYield`

**historical_price_adjusted:** `date`, `adjClose`, `volume`

**treasury_rates:** `date`, `month3`, `year2`, `year10`, `year30`

**dividends:** `date`, `adjDividend`

**analyst_estimates:** `date`, `revenueAvg`, `revenueLow`, `revenueHigh`, `epsAvg`, `epsLow`, `epsHigh`, `numAnalystsRevenue`, `numAnalystsEps`

**price_target:** `lastMonthCount`, `lastMonthAvgPriceTarget`, `lastQuarterCount`, `lastQuarterAvgPriceTarget`, `lastYearCount`, `lastYearAvgPriceTarget`

**price_target_consensus:** `targetHigh`, `targetLow`, `targetConsensus`, `targetMedian`

**earnings_transcript:** `symbol`, `quarter`, `year`, `date`, `content` (full transcript text)

**sec_filings:** `symbol`, `fillingDate`, `acceptedDate`, `cik`, `type`, `link`, `finalLink` (link to SEC.gov filing)

## Tips

1. **For sub-agents:** Start prompts with "First, read `fmp/AGENT_PROMPTING_GUIDE.md` and `fmp/API_REFERENCE.json`" — this gives the agent full context on available endpoints and usage patterns
2. Always use `fmp.fetch("endpoint_name", symbol="TICKER", ...)` syntax
2. Import `FMPEmptyResponseError` for endpoints that may return no data
3. All responses are **pandas DataFrames** — use `df.empty`, `df.iterrows()`, `df.iloc[0]`
4. Use `profile` first to confirm ticker maps to expected company
5. Specify exact endpoint names from the table
6. List symbols explicitly
7. Mention expected columns to reduce errors
8. Ask comparative questions ("which is best?")
9. Request tables/summaries for clean output

## Important Notes

- **Return type:** All `fmp.fetch()` calls return pandas DataFrames, not dicts or lists
- **Date params:** Use `**{"from": "2024-01-01", "to": "2024-12-31"}` for date range params (since `from` is a Python keyword)
- **Empty responses:** Some endpoints (like `dividends`) raise `FMPEmptyResponseError` when no data exists
- **Sorting:** DataFrames often come in reverse chronological order; use `df.sort_values('date')` if needed
- **SEC filing content:** The `sec_filings` endpoint returns links to SEC.gov. To fetch content, use `requests` with a User-Agent header: `headers = {'User-Agent': 'Name (email)'}`
