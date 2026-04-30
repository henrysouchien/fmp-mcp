# Changelog

All notable changes to `fmp-mcp` are documented here. Entries follow the
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) convention.

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
