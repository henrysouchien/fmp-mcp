# Changelog

All notable changes to `fmp-mcp` are documented here. Entries follow the
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) convention.

## [0.5.9] — 2026-11-19

### Added
- Added `configure_cache_base()` so a host application can explicitly bind the FMP cache and time-series store root. Existing environment and XDG cache defaults remain unchanged for standalone consumers.
- Registered two stable endpoints: `available_exchanges` (`/stable/available-exchanges`: FMP's exchange list with each code's name, country, `symbolSuffix` and delay; optional `extended`) and `symbol_search` (`/stable/search-symbol`: `query`, `limit`, `exchange`). Neither is cached. The legacy `search` endpoint and its `search_symbol` alias are unchanged.
- `FMPTimeoutError` and `FMPConnectionError`, both subclasses of `FMPAPIError`: `FMPClient.fetch` raises them for a request that timed out and for a connection that failed or was refused, where it raised a bare `FMPAPIError` with the same message. A caller can tell an outage that waiting may clear by its type, never by its words; the transport's error is still not chained. Any other transport failure (a bad URL, too many redirects, a broken body) still raises `FMPAPIError`. Every caller that catches `FMPAPIError` catches both unchanged.
- `fmp.figure_statements` and `FMPFetchResult.statements` / `VendorResponse`: the single canonical owner stating what an FMP figure measures (`{measure, period, basis}` under `value_semantics_core.figure_statement`). Exports `FMP_FIELD_CLASSIFICATIONS`, `fmp_field_classification`, `fmp_record_fiscal_year`, `fmp_record_period_end`, `fmp_row_currency`, `vendor_measure`, `vendor_basis`, `row_period`, `vendor_figure_statement`, `computed_statement`, `trailing_four_quarter_sum`, `vendor_row_statements`, `statements_table`, `VENDOR_UNCOMPARED_BASIS`, and `BASIS_NOT_RECORDED`. `fmp_fetch`, `compare_peers`, and `get_stock_fundamentals` attach validated `statements` entries (omitting `statements` when empty).

### Changed
- Raised `value-semantics-core` dependency floor from `>=2.1,<3` to `>=2.3.1,<3` (matching the 3-fact `figure_statement` contract imported by `fmp.figure_statements`) and pinned the `[build-system]` backend to `hatchling==1.33.0`.
- An answer no longer names an environment setting. With no FMP key, `FMPClient` raises `FMPAuthenticationError("Market data from Financial Modeling Prep is not configured on this installation, so nothing was read. It is set up by whoever runs this installation.")` (`fmp.client.FMP_NOT_CONFIGURED`; was "FMP_API_KEY not found in environment"); the estimate-revision tools with an empty estimates address answer "Estimate revisions are not configured on this installation, so nothing was read. They are set up by whoever runs this installation." (`fmp.estimates_client.ESTIMATES_NOT_CONFIGURED`, replacing `MISSING_API_URL_ERROR`); a transcript whose corpus save is switched on with no corpus location answers "The transcript was not saved: saving transcripts to the research library is switched on but not set up on this installation. It is set up by whoever runs this installation." (`fmp.tools.transcripts.CORPUS_NOT_SET_UP`). Each setting is logged at a warning where the call is refused. Types and error codes are unchanged.

## [0.5.8] — 2026-10-23

### Changed
- `configure_client()` called without a `budget_guard` now installs `fmp.spend`, so every FMP network call in a host that injects no guard writes one row to the machine-wide API spend ledger (`api_category` `market_data`, provider `fmp`/`fmp_estimates`, endpoint `<provider>:<operation>`, key fingerprint, ticker, HTTP status, bytes, duration; a 429 retry is its own row) and every cache-answered request writes a zero-call `cache_hit` row. A call whose spend ceiling (`freeze_payg`, `market_data_daily_calls_cap`, …) is already met raises `SpendCeilingExceeded` before any request is sent and writes a `budget_blocked` row. A host `budget_guard` (Risk's) still owns metering and nothing from `fmp.spend` runs; a host `event_observer` without a guard receives events alongside the ledger observer.
- New dependency `hank-model-authority>=1.7.1,<2` (the ledger writer).

## [0.5.7] — 2026-10-23

### Added
- `FMPClient.fetch` and `fetch_with_lineage` report a request answered from the disk cache to the configured event observer as `cache_hit` (endpoint name, validated `params`), so a host can count cache hits beside network calls. No new dependency; without an observer nothing changes.

## [0.5.6] — 2026-09-16

### Fixed
- Excludes PyArrow 25.0.0, whose bundled allocator crashes when first imported on a worker thread that exits (Apache Arrow #50471). PyArrow 25.0.1 remains supported.

## [0.5.5] — 2026-09-16

### Changed
- Admits PyArrow 23–25 (`>=23.0.1,<26`) with unchanged parquet cache behavior.

## [0.5.4] — 2026-09-16

### Fixed
- Optional PostgreSQL driver imports occur only when using `EstimateStore` database operations; every shipped library module imports without the `estimates` extra.
- Supersedes 0.5.3, whose registry-only import proof exposed the eager optional-driver dependency.

## [0.5.3] — 2026-09-16

### Changed
- Supports both FastMCP 3 and 4 with `fastmcp>=3.1,<5`; the MCP tool surface is unchanged.
- Source is a nested, installable `fmp-mcp` package; the Risk root no longer claims `fmp`.
- Shared helpers and standalone imports are owned in source rather than synthesized during public sync.
- Application budget, telemetry, peer discovery and FX policies are supplied through explicit startup callbacks; the public server reads its process environment without checkout discovery.
- Risk-only compatibility/FX wrappers and the snapshot launcher moved to the application. Public sync excludes the two Hank-only tool modules and package tests.
- Distribution sync mirrors only the package subtree with deletion, leaving repository metadata and published artifacts untouched.
- Declares the public `value-semantics-core>=2.1,<3` dependency, so registry-only installs import the shipped helpers without a Hank checkout.

## [0.5.0] — 2026-08-20

### Added
- **`fmp/definitions.py`, `fmp/lineage.py`, and the `fmp/definition_artifacts/` data directory now ship in the published package.** They existed in the monorepo source but the sync had not been re-run since they landed, so the 0.4.4 artifact did not contain them.
- The following names are now importable from `fmp`: `get_fmp_field_definition`, `FMPFieldDefinition`, `FMPFieldDefinitionAvailable`, `FMPFieldDefinitionUnavailable`, `FMPDefinitionArtifactError`, `fetch_with_lineage`, `FMPFetchResult`.

All seven names were already declared in `fmp/__init__.py`'s `__all__` but were absent from the 0.4.4 artifact, so installed consumers raised `ImportError` — concretely `ai-excel-addin/api/research/fmp_definition_evidence.py`, which does `from fmp import get_fmp_field_definition`, could not import against a clean `pip install fmp-mcp`. A source/distribution parity test (`risk_module/tests/fmp/test_public_package_parity.py`) now fails on this class of drift.

## [0.4.1] — 2026-07-08

### Fixed
- **`fmp/tools/_helpers.py` no longer imports `utils.numeric`** — `safe_float` / `safe_float_or_none` are vendored directly into the module. The 0.4.0 wheel was import-broken outside the monorepo (`ModuleNotFoundError: No module named 'utils'` on `import fmp.server`) because `utils.numeric` is not in the sync script's vendoring whitelist and itself re-exports from the non-vendored `portfolio_math` package.

## [0.4.0] — 2026-07-07

### Changed
- Expanded market, transcripts, insider, institutional, and ETF/funds tools; new `fmp/registry.py` manifest dispatcher. (Published without a changelog entry; this summary reconstructed from the sync diff. Import-broken outside the monorepo — fixed in 0.4.1.)

## [0.3.3] — 2026-04-30

### Fixed
Cross-package boundary breakages that prevented `import fmp.server` from succeeding in a clean Python environment (e.g. fresh `pip install fmp-mcp` outside the monorepo). Three independent fixes shipped together:

- **`utils/timeseries_store.py` and `utils/fmp_helpers.py` are now vendored into `fmp/_shared/`** at sync time, with imports rewritten from `utils.X` → `fmp._shared.X`. Prior versions assumed the monorepo `utils/` package would be on `sys.path`, which it isn't for users installing from PyPI.
- **`app_platform.api_budget.guard_call` import is wrapped in `try/except ImportError`** at each of its call sites (`fmp/client.py:30`, `fmp/estimates_client.py:12`). When `app_platform` isn't installed, a no-op passthrough fallback is used. Standalone installs run without monorepo budget enforcement; monorepo runtime is unchanged.
- **`core.corpus.{db,ingest}` imports moved out of `fmp/tools/transcripts.py` module top into `_ingest_transcript_result` itself** (the only function that uses them, gated by env vars `CORPUS_ROOT`, `CORPUS_DB_PATH`, `CORPUS_INGEST_ENABLED`). Module load no longer requires monorepo `core/` to be importable.

### Other notes
Optional `utils.logging` imports for observability hooks (`fmp/client.py:319,334,349`) remain `try/except`-wrapped — they fail-silently in standalone installs and are designed to do so.

## [0.3.2]

### Changed
- Added version upper bounds to all dependencies in `pyproject.toml`.

## [0.3.1]

### Changed
- PyPI license metadata republish (no functional changes).

## [0.3.0]

### Changed
- License switched from MIT to PolyForm Noncommercial 1.0.0.
- Estimate tools converted to HTTP-only — local `EstimateStore` fallback removed.

## [0.2.0]

### Added
- HTTP API path for estimate tools.

## [0.1.0]

### Added
- Initial release.
