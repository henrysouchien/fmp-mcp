# TODO - fmp-mcp

## Active

No active items.

## Resolved

| ID | Area | Resolution | Released |
|---|---|---|---|
| FX1 | Historical FX source basis | `historical_price_eod` responses now expose provider, market-close basis, adjustment status, FX pair direction/rate semantics, and explicit non-equivalence to official central-bank, local interbank, and management-guidance rates. Endpoint discovery carries the same distinction. A dedicated multi-provider FX selector was intentionally not added because this package only owns FMP data; authoritative alternatives must be sourced and cited by the consumer. | `fmp-mcp` 0.4.3, 2026-07-15 |
