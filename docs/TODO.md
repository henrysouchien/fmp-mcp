# TODO - fmp-mcp

## Active

| ID | Area | Status | Next action | Source |
|---|---|---|---|---|
| FX1 | Historical FX source basis | `OPEN 2026-05-12` | Expose source/basis metadata for `historical_price_eod` FX pairs and evaluate a dedicated official-FX helper for accounting-style currency bridges. VAL q012 showed `USDTWD` via FMP returned 32.768 for 2025-03-11, while Taiwan CBC interbank spot closing data published 32.884. Agents using FMP as the sole exact-date FX source understated the local-currency guidance midpoint miss. | Risk-module VAL q012; Taiwan CBC 2025 NTD/USD historical data |
