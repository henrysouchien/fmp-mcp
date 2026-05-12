# TODO - fmp-mcp

## Active

| ID | Area | Status | Next action | Source |
|---|---|---|---|---|
| FX1 | Historical FX source basis | `OPEN 2026-05-12` | Expose source/basis metadata for `historical_price_eod` FX pairs and evaluate whether a dedicated FX-basis helper should let agents request market-data close, official central-bank rate, local interbank close, or management-guidance FX explicitly. VAL q012 showed `USDTWD` via FMP returned 32.768 for 2025-03-11, while Taiwan CBC interbank spot closing data published 32.884 and the rubric expected about 32.88. This does not mean the FMP value is wrong; the gap is that agents cannot see or select the intended FX basis cleanly. | Risk-module VAL q012; Taiwan CBC 2025 NTD/USD historical data |
