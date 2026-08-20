# Changelog

All notable changes to `fmp-mcp` are documented here. Entries follow the
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) convention.

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
