"""Postgres store for tracking analyst estimate revisions over time."""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, ClassVar, Optional

import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import Json, RealDictCursor
from psycopg2.extensions import connection as PsycopgConnection
from psycopg2.pool import SimpleConnectionPool


_DEFAULT_DATABASE_URL = "postgresql://postgres@localhost:5432/fmp_data_db"
_SCHEMA_PATH = Path(__file__).resolve().parent / "scripts" / "create_fmp_data_schema.sql"


def _utc_now() -> datetime:
    """Return current UTC datetime."""
    return datetime.now(timezone.utc)


def _utc_today() -> date:
    """Return current UTC date."""
    return _utc_now().date()


def _as_float(value: Any) -> float | None:
    """Best-effort float parsing for estimate payload fields."""
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    """Best-effort integer parsing for estimate payload fields."""
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _chunks(items: list[str], size: int) -> list[list[str]]:
    """Chunk a list into fixed-size slices."""
    if size <= 0:
        return [items]
    return [items[i: i + size] for i in range(0, len(items), size)]


def _parse_iso_date(value: Any) -> date | None:
    """Best-effort ISO date parsing for fiscal_date fields."""
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).date() if value.tzinfo else value.date()
    if isinstance(value, date):
        return value

    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


class EstimateStore:
    """Postgres-backed storage for estimate snapshots and revision analytics."""

    _reader_pools: ClassVar[dict[str, SimpleConnectionPool]] = {}

    def __init__(self, database_url: str | None = None, read_only: bool = False):
        self.database_url = (database_url or os.getenv("FMP_DATA_DATABASE_URL") or _DEFAULT_DATABASE_URL).strip()
        self.read_only = read_only
        self._available = True
        self.conn: PsycopgConnection | None = None
        self._reader_pool: SimpleConnectionPool | None = None

        try:
            if self.read_only:
                self._reader_pool = self._get_reader_pool(self.database_url)
                self.conn = self._reader_pool.getconn()
                self.conn.autocommit = True
                self._apply_session_settings(self.conn, read_only=True)
            else:
                self.conn = psycopg2.connect(self.database_url, cursor_factory=RealDictCursor)
                self.conn.autocommit = False
                self._apply_session_settings(self.conn, read_only=False)
                self._ensure_schema()
        except OperationalError:
            if self.read_only:
                self._available = False
                self.conn = None
                return
            raise

    @classmethod
    def _get_reader_pool(cls, database_url: str) -> SimpleConnectionPool:
        pool = cls._reader_pools.get(database_url)
        if pool is None:
            pool = SimpleConnectionPool(1, 3, database_url, cursor_factory=RealDictCursor)
            cls._reader_pools[database_url] = pool
        return pool

    @staticmethod
    def _apply_session_settings(conn: PsycopgConnection, read_only: bool) -> None:
        with conn.cursor() as cur:
            cur.execute("SET timezone = 'UTC'")
            if read_only:
                cur.execute("SET default_transaction_read_only = true")

        if not conn.autocommit:
            conn.commit()

    def _ensure_schema(self) -> None:
        self._require_write()
        assert self.conn is not None

        schema_sql = _SCHEMA_PATH.read_text(encoding="utf-8")
        with self.conn.cursor() as cur:
            cur.execute(schema_sql)
        self.conn.commit()

    def close(self) -> None:
        if self.conn is None:
            return

        if self.read_only and self._reader_pool is not None:
            self._reader_pool.putconn(self.conn)
        else:
            self.conn.close()
        self.conn = None

    def __enter__(self) -> "EstimateStore":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _require_write(self) -> None:
        if self.read_only:
            raise RuntimeError("EstimateStore is read-only")
        if self.conn is None:
            raise RuntimeError("EstimateStore is not available")

    def _ensure_available(self) -> bool:
        return self._available and self.conn is not None

    @staticmethod
    def _clean_period(period: str) -> str:
        value = (period or "quarter").strip().lower()
        if value not in {"annual", "quarter"}:
            raise ValueError("period must be 'annual' or 'quarter'")
        return value

    @staticmethod
    def _clean_error_type(error_type: str) -> str:
        value = (error_type or "").strip().lower()
        if value not in {"no_income_statement", "no_estimates", "api_error", "unknown"}:
            raise ValueError(
                "error_type must be one of: "
                "'no_income_statement', 'no_estimates', 'api_error', 'unknown'"
            )
        return value

    @staticmethod
    def _pick(data: dict[str, Any], keys: list[str]) -> Any:
        for key in keys:
            value = data.get(key)
            if value not in (None, ""):
                return value
        return None

    @staticmethod
    def _serialize_value(value: Any) -> Any:
        if isinstance(value, datetime):
            dt = value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
            return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, Decimal):
            return float(value)
        return value

    @classmethod
    def _format_snapshot_row(cls, row: dict[str, Any]) -> dict[str, Any]:
        data = dict(row)
        for key, value in data.items():
            data[key] = cls._serialize_value(value)
        return data

    @classmethod
    def _format_run_row(cls, row: dict[str, Any]) -> dict[str, Any]:
        data = dict(row)
        for key, value in data.items():
            data[key] = cls._serialize_value(value)
        return data

    @staticmethod
    def _clean_universe_source(universe_source: str | None) -> str:
        source = (universe_source or "screener").strip().lower()
        if source not in {"bulk", "screener", "explicit"}:
            return "screener"
        return source

    def create_run(self, universe: list[str], universe_source: str = "screener") -> int:
        """Create a new snapshot run and return run_id."""
        self._require_write()
        assert self.conn is not None

        clean_universe = sorted({str(t).strip().upper() for t in universe if str(t).strip()})
        clean_source = self._clean_universe_source(universe_source)

        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO snapshot_runs (started_at, status, universe_snapshot, universe_source)
                    VALUES (%s, 'running', %s, %s)
                    RETURNING id
                    """,
                    (_utc_now(), Json(clean_universe), clean_source),
                )
                row = cur.fetchone()

        if row is None:
            raise RuntimeError("Failed to create snapshot run")
        return int(row["id"])

    def update_run(self, run_id: int, **kwargs: Any) -> None:
        """Update fields on an existing run row."""
        self._require_write()
        assert self.conn is not None

        allowed = {
            "started_at",
            "completed_at",
            "status",
            "tickers_attempted",
            "tickers_succeeded",
            "tickers_failed",
            "rows_inserted",
            "last_ticker_processed",
            "error_message",
            "universe_snapshot",
            "universe_source",
        }

        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return

        values: list[Any] = []
        set_clause_parts: list[str] = []
        for column, value in updates.items():
            set_clause_parts.append(f"{column}=%s")
            if column == "universe_snapshot" and not isinstance(value, str):
                values.append(Json(value))
            elif column == "universe_source":
                values.append(self._clean_universe_source(value))
            elif isinstance(value, (dict, list)):
                values.append(Json(value))
            else:
                values.append(value)

        values.append(run_id)
        set_clause = ", ".join(set_clause_parts)

        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(
                    f"UPDATE snapshot_runs SET {set_clause} WHERE id=%s",
                    values,
                )

    def save_snapshots(
        self,
        run_id: int,
        ticker: str,
        estimates: list[dict[str, Any]],
        period: str = "quarter",
        snapshot_date: date | None = None,
    ) -> int:
        """Persist estimate rows for a single ticker; returns inserted row count.

        If ``snapshot_date`` is provided it is used for all rows, ensuring a
        consistent date across an entire run even if execution crosses midnight.
        """
        self._require_write()
        assert self.conn is not None

        clean_ticker = str(ticker).strip().upper()
        clean_period = self._clean_period(period)
        snapshot_date = snapshot_date or _utc_today()

        insert_sql = """
            INSERT INTO estimate_snapshots (
                run_id,
                ticker,
                fiscal_date,
                period,
                snapshot_date,
                eps_avg,
                eps_high,
                eps_low,
                num_analysts_eps,
                revenue_avg,
                revenue_high,
                revenue_low,
                num_analysts_revenue,
                ebitda_avg,
                ebitda_high,
                ebitda_low,
                net_income_avg,
                net_income_high,
                net_income_low,
                ebit_avg,
                ebit_high,
                ebit_low,
                sga_expense_avg,
                sga_expense_high,
                sga_expense_low,
                raw_data
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker, fiscal_date, period, snapshot_date) DO NOTHING
        """

        inserted = 0
        with self.conn:
            with self.conn.cursor() as cur:
                for estimate in estimates:
                    if not isinstance(estimate, dict):
                        continue

                    fiscal_date_value = self._pick(estimate, ["fiscal_date", "fiscalDate", "date"])
                    fiscal_date = _parse_iso_date(fiscal_date_value)
                    if fiscal_date is None:
                        continue

                    cur.execute(
                        insert_sql,
                        (
                            run_id,
                            clean_ticker,
                            fiscal_date,
                            clean_period,
                            snapshot_date,
                            _as_float(self._pick(estimate, ["eps_avg", "epsAvg"])),
                            _as_float(self._pick(estimate, ["eps_high", "epsHigh"])),
                            _as_float(self._pick(estimate, ["eps_low", "epsLow"])),
                            _as_int(self._pick(estimate, ["num_analysts_eps", "numAnalystsEps", "numAnalystsEPS"])),
                            _as_float(self._pick(estimate, ["revenue_avg", "revenueAvg"])),
                            _as_float(self._pick(estimate, ["revenue_high", "revenueHigh"])),
                            _as_float(self._pick(estimate, ["revenue_low", "revenueLow"])),
                            _as_int(
                                self._pick(
                                    estimate,
                                    [
                                        "num_analysts_revenue",
                                        "numAnalystsRevenue",
                                    ],
                                )
                            ),
                            _as_float(self._pick(estimate, ["ebitda_avg", "ebitdaAvg"])),
                            _as_float(self._pick(estimate, ["ebitda_high", "ebitdaHigh"])),
                            _as_float(self._pick(estimate, ["ebitda_low", "ebitdaLow"])),
                            _as_float(self._pick(estimate, ["net_income_avg", "netIncomeAvg"])),
                            _as_float(self._pick(estimate, ["net_income_high", "netIncomeHigh"])),
                            _as_float(self._pick(estimate, ["net_income_low", "netIncomeLow"])),
                            _as_float(self._pick(estimate, ["ebit_avg", "ebitAvg"])),
                            _as_float(self._pick(estimate, ["ebit_high", "ebitHigh"])),
                            _as_float(self._pick(estimate, ["ebit_low", "ebitLow"])),
                            _as_float(self._pick(estimate, ["sga_expense_avg", "sgaExpenseAvg"])),
                            _as_float(self._pick(estimate, ["sga_expense_high", "sgaExpenseHigh"])),
                            _as_float(self._pick(estimate, ["sga_expense_low", "sgaExpenseLow"])),
                            Json(estimate),
                        ),
                    )
                    inserted += cur.rowcount

        return inserted

    def record_failure(
        self,
        run_id: int,
        ticker: str,
        error_type: str,
        period: str | None = None,
        error_message: str | None = None,
    ) -> None:
        """Record a per-ticker collection failure."""
        self._require_write()
        assert self.conn is not None

        clean_ticker = str(ticker).strip().upper()
        if not clean_ticker:
            raise ValueError("ticker cannot be empty")

        clean_error_type = self._clean_error_type(error_type)
        clean_period = self._clean_period(period) if period is not None else None

        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO collection_failures (run_id, ticker, period, error_type, error_message)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (run_id, clean_ticker, clean_period, clean_error_type, error_message),
                )

    def get_resumable_run(self) -> Optional[dict[str, Any]]:
        """Return most recent run with status running/partial, if any."""
        if not self._ensure_available():
            return None
        assert self.conn is not None

        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM snapshot_runs
                WHERE status IN ('running', 'partial')
                ORDER BY id DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()

        if row is None:
            return None
        return self._format_run_row(row)

    def get_latest(self, ticker: str, period: str = "quarter") -> list[dict[str, Any]]:
        """Get latest snapshot row per fiscal_date for a ticker."""
        if not self._ensure_available():
            return []
        assert self.conn is not None

        clean_ticker = str(ticker).strip().upper()
        clean_period = self._clean_period(period)

        with self.conn.cursor() as cur:
            cur.execute(
                """
                WITH latest AS (
                    SELECT fiscal_date, MAX(snapshot_date) AS max_snapshot_date
                    FROM estimate_snapshots
                    WHERE ticker = %s AND period = %s
                    GROUP BY fiscal_date
                )
                SELECT s.*
                FROM estimate_snapshots s
                JOIN latest l
                  ON s.fiscal_date = l.fiscal_date
                 AND s.snapshot_date = l.max_snapshot_date
                WHERE s.ticker = %s AND s.period = %s
                ORDER BY s.fiscal_date
                """,
                (clean_ticker, clean_period, clean_ticker, clean_period),
            )
            rows = cur.fetchall()

        return [self._format_snapshot_row(row) for row in rows]

    def get_revisions(
        self,
        ticker: str,
        fiscal_date: str,
        period: str = "quarter",
    ) -> list[dict[str, Any]]:
        """Get all snapshots for ticker + fiscal period ordered by snapshot_date."""
        if not self._ensure_available():
            return []
        assert self.conn is not None

        clean_ticker = str(ticker).strip().upper()
        clean_period = self._clean_period(period)
        clean_fiscal = _parse_iso_date(fiscal_date)
        if clean_fiscal is None:
            return []

        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM estimate_snapshots
                WHERE ticker = %s
                  AND fiscal_date = %s
                  AND period = %s
                ORDER BY snapshot_date
                """,
                (clean_ticker, clean_fiscal, clean_period),
            )
            rows = cur.fetchall()

        return [self._format_snapshot_row(row) for row in rows]

    @staticmethod
    def _delta(current: float | None, baseline: float | None) -> float | None:
        if current is None or baseline is None:
            return None
        return current - baseline

    @staticmethod
    def _direction(eps_delta: float | None, revenue_delta: float | None) -> str:
        signal = eps_delta if eps_delta is not None else revenue_delta
        if signal is None:
            return "unknown"
        if signal > 0:
            return "up"
        if signal < 0:
            return "down"
        return "flat"

    def get_revision_summary(
        self,
        tickers: list[str] | None,
        days: int = 30,
        period: str = "quarter",
    ) -> list[dict[str, Any]]:
        """Compare latest vs lookback snapshot for each ticker's nearest fiscal period."""
        if not self._ensure_available():
            return []
        assert self.conn is not None

        if days < 0:
            raise ValueError("days must be non-negative")

        clean_period = self._clean_period(period)
        today_utc = _utc_today()
        cutoff_date = today_utc - timedelta(days=days)

        where_filter = ""
        params: list[Any] = [clean_period]

        if tickers:
            clean_tickers = sorted({str(t).strip().upper() for t in tickers if str(t).strip()})
            if not clean_tickers:
                return []
            placeholders = ",".join("%s" for _ in clean_tickers)
            where_filter = f" AND ticker IN ({placeholders})"
            params.extend(clean_tickers)

        query = f"""
            WITH latest_snapshot AS (
                SELECT ticker, MAX(snapshot_date) AS latest_snapshot_date
                FROM estimate_snapshots
                WHERE period = %s {where_filter}
                GROUP BY ticker
            ),
            latest_fiscal AS (
                SELECT
                    ls.ticker,
                    ls.latest_snapshot_date,
                    (
                        SELECT s.fiscal_date
                        FROM estimate_snapshots s
                        WHERE s.ticker = ls.ticker
                          AND s.period = %s
                          AND s.snapshot_date = ls.latest_snapshot_date
                        ORDER BY
                            CASE WHEN s.fiscal_date >= %s THEN 0 ELSE 1 END,
                            ABS(s.fiscal_date - %s),
                            s.fiscal_date
                        LIMIT 1
                    ) AS fiscal_date
                FROM latest_snapshot ls
            ),
            latest_rows AS (
                SELECT s.*
                FROM estimate_snapshots s
                JOIN latest_fiscal lf
                  ON s.ticker = lf.ticker
                 AND s.fiscal_date = lf.fiscal_date
                 AND s.snapshot_date = lf.latest_snapshot_date
                WHERE s.period = %s
            ),
            baseline_dates AS (
                SELECT
                    lf.ticker,
                    lf.fiscal_date,
                    MAX(s.snapshot_date) AS baseline_snapshot_date
                FROM latest_fiscal lf
                LEFT JOIN estimate_snapshots s
                  ON s.ticker = lf.ticker
                 AND s.period = %s
                 AND s.fiscal_date = lf.fiscal_date
                 AND s.snapshot_date <= %s
                GROUP BY lf.ticker, lf.fiscal_date
            ),
            baseline_rows AS (
                SELECT s.*
                FROM estimate_snapshots s
                JOIN baseline_dates bd
                  ON s.ticker = bd.ticker
                 AND s.fiscal_date = bd.fiscal_date
                 AND s.snapshot_date = bd.baseline_snapshot_date
                WHERE s.period = %s
            )
            SELECT
                l.ticker,
                l.fiscal_date,
                l.snapshot_date AS latest_snapshot_date,
                b.snapshot_date AS baseline_snapshot_date,
                l.eps_avg AS latest_eps_avg,
                b.eps_avg AS baseline_eps_avg,
                l.revenue_avg AS latest_revenue_avg,
                b.revenue_avg AS baseline_revenue_avg
            FROM latest_rows l
            LEFT JOIN baseline_rows b
              ON b.ticker = l.ticker
             AND b.fiscal_date = l.fiscal_date
            ORDER BY l.ticker
        """

        params.extend([clean_period, today_utc, today_utc, clean_period, clean_period, cutoff_date, clean_period])

        with self.conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

        results: list[dict[str, Any]] = []
        for row in rows:
            data = self._format_snapshot_row(row)
            eps_delta = self._delta(data.get("latest_eps_avg"), data.get("baseline_eps_avg"))
            revenue_delta = self._delta(data.get("latest_revenue_avg"), data.get("baseline_revenue_avg"))

            results.append(
                {
                    "ticker": data.get("ticker"),
                    "fiscal_date": data.get("fiscal_date"),
                    "latest_snapshot_date": data.get("latest_snapshot_date"),
                    "baseline_snapshot_date": data.get("baseline_snapshot_date"),
                    "latest_eps_avg": data.get("latest_eps_avg"),
                    "baseline_eps_avg": data.get("baseline_eps_avg"),
                    "eps_delta": eps_delta,
                    "latest_revenue_avg": data.get("latest_revenue_avg"),
                    "baseline_revenue_avg": data.get("baseline_revenue_avg"),
                    "revenue_delta": revenue_delta,
                    "direction": self._direction(eps_delta, revenue_delta),
                }
            )

        return results

    def get_freshness(
        self, tickers: list[str], period: str | None = None,
    ) -> dict[str, Optional[str]]:
        """Return {ticker: latest_snapshot_date} for the requested ticker list.

        If ``period`` is given, only consider snapshots of that period.
        """
        if not tickers:
            return {}

        clean_tickers = sorted({str(t).strip().upper() for t in tickers if str(t).strip()})
        if not clean_tickers:
            return {}

        if not self._ensure_available():
            return {ticker: None for ticker in clean_tickers}
        assert self.conn is not None

        period_filter = ""
        period_params: list[Any] = []
        if period:
            clean_period = self._clean_period(period)
            period_filter = " AND period = %s"
            period_params = [clean_period]

        freshness_map: dict[str, str | None] = {ticker: None for ticker in clean_tickers}
        for batch in _chunks(clean_tickers, 1000):
            placeholders = ",".join("%s" for _ in batch)
            with self.conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT ticker, MAX(snapshot_date) AS latest_snapshot_date
                    FROM estimate_snapshots
                    WHERE ticker IN ({placeholders}){period_filter}
                    GROUP BY ticker
                    """,
                    batch + period_params,
                )
                rows = cur.fetchall()

            for row in rows:
                data = self._format_snapshot_row(row)
                freshness_map[str(data["ticker"])] = data.get("latest_snapshot_date")

        return freshness_map

    def get_failure_summary(self, min_runs: int = 1) -> list[dict[str, Any]]:
        """Return grouped failure stats by ticker, period, and error type."""
        if min_runs < 1:
            raise ValueError("min_runs must be at least 1")
        if not self._ensure_available():
            return []
        assert self.conn is not None

        with self.conn.cursor() as cur:
            cur.execute(
                """
                WITH grouped AS (
                    SELECT
                        ticker,
                        period,
                        error_type,
                        COUNT(DISTINCT run_id) AS distinct_run_count,
                        COUNT(*) AS total_failure_count
                    FROM collection_failures
                    GROUP BY ticker, period, error_type
                    HAVING COUNT(DISTINCT run_id) >= %s
                )
                SELECT
                    g.ticker,
                    g.period,
                    g.error_type,
                    g.distinct_run_count,
                    g.total_failure_count,
                    latest.run_id AS latest_run_id,
                    latest.error_message AS latest_error_message
                FROM grouped g
                LEFT JOIN LATERAL (
                    SELECT run_id, error_message
                    FROM collection_failures cf
                    WHERE cf.ticker = g.ticker
                      AND cf.error_type = g.error_type
                      AND cf.period IS NOT DISTINCT FROM g.period
                    ORDER BY cf.created_at DESC, cf.id DESC
                    LIMIT 1
                ) latest ON TRUE
                ORDER BY
                    g.distinct_run_count DESC,
                    g.total_failure_count DESC,
                    g.ticker,
                    g.period NULLS FIRST,
                    g.error_type
                """,
                (min_runs,),
            )
            rows = cur.fetchall()

        return [self._format_run_row(row) for row in rows]

    def list_tickers(self) -> list[str]:
        """List all tickers with at least one snapshot."""
        if not self._ensure_available():
            return []
        assert self.conn is not None

        with self.conn.cursor() as cur:
            cur.execute("SELECT DISTINCT ticker FROM estimate_snapshots ORDER BY ticker")
            rows = cur.fetchall()

        return [str(row["ticker"]) for row in rows]
