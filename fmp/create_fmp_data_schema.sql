-- Earnings estimate schema for fmp_data_db.
--
-- CANONICAL COPY. Also deployed to edgar_updater/estimates/scripts/ for EC2/RDS.
-- The primary database is now RDS (via edgar_updater). This local copy is kept
-- as the source of truth for schema changes.

CREATE TABLE IF NOT EXISTS snapshot_runs (
    id SERIAL PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'completed', 'failed', 'partial')),
    tickers_attempted INTEGER DEFAULT 0,
    tickers_succeeded INTEGER DEFAULT 0,
    tickers_failed INTEGER DEFAULT 0,
    rows_inserted INTEGER DEFAULT 0,
    last_ticker_processed TEXT,
    error_message TEXT,
    universe_snapshot JSONB,
    universe_source TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE snapshot_runs
    ADD COLUMN IF NOT EXISTS universe_source TEXT;

CREATE TABLE IF NOT EXISTS estimate_snapshots (
    id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES snapshot_runs(id),
    ticker TEXT NOT NULL,
    fiscal_date DATE NOT NULL,
    period TEXT NOT NULL CHECK (period IN ('annual', 'quarter')),
    snapshot_date DATE NOT NULL,

    eps_avg NUMERIC(20, 6),
    eps_high NUMERIC(20, 6),
    eps_low NUMERIC(20, 6),
    num_analysts_eps INTEGER,

    revenue_avg NUMERIC(20, 2),
    revenue_high NUMERIC(20, 2),
    revenue_low NUMERIC(20, 2),
    num_analysts_revenue INTEGER,

    ebitda_avg NUMERIC(20, 2),
    ebitda_high NUMERIC(20, 2),
    ebitda_low NUMERIC(20, 2),

    net_income_avg NUMERIC(20, 2),
    net_income_high NUMERIC(20, 2),
    net_income_low NUMERIC(20, 2),

    ebit_avg NUMERIC(20, 2),
    ebit_high NUMERIC(20, 2),
    ebit_low NUMERIC(20, 2),

    sga_expense_avg NUMERIC(20, 2),
    sga_expense_high NUMERIC(20, 2),
    sga_expense_low NUMERIC(20, 2),

    raw_data JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (ticker, fiscal_date, period, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_est_ticker_period_snap
    ON estimate_snapshots(ticker, period, snapshot_date DESC);
CREATE INDEX IF NOT EXISTS idx_est_ticker_fiscal
    ON estimate_snapshots(ticker, fiscal_date);
CREATE INDEX IF NOT EXISTS idx_est_run_id
    ON estimate_snapshots(run_id);
CREATE INDEX IF NOT EXISTS idx_est_snapshot_date
    ON estimate_snapshots(snapshot_date);

CREATE TABLE IF NOT EXISTS collection_failures (
    id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES snapshot_runs(id),
    ticker TEXT NOT NULL,
    period TEXT CHECK (period IS NULL OR period IN ('annual', 'quarter')),
    error_type TEXT NOT NULL CHECK (error_type IN ('no_income_statement', 'no_estimates', 'api_error', 'unknown')),
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cf_ticker
    ON collection_failures(ticker);
CREATE INDEX IF NOT EXISTS idx_cf_run_id
    ON collection_failures(run_id);
CREATE INDEX IF NOT EXISTS idx_cf_error_type
    ON collection_failures(error_type);
