"""Shared helpers for normalizing FMP payload values."""

from __future__ import annotations

from datetime import date
from typing import Any, Optional


def parse_fmp_float(value: Any) -> Optional[float]:
    """Convert FMP numeric payload values into finite float values."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        converted = float(value)
        return converted if converted == converted else None  # NaN check
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        wrapped_negative = text.startswith("(") and text.endswith(")")
        cleaned = (
            text.replace("%", "")
            .replace(",", "")
            .replace("(", "")
            .replace(")", "")
        )
        if cleaned.startswith("+"):
            cleaned = cleaned[1:]
        try:
            converted = float(cleaned)
            if wrapped_negative and converted > 0:
                converted = -converted
            return converted if converted == converted else None  # NaN check
        except ValueError:
            return None
    return None


def pick_value(record: dict[str, Any], *keys: str) -> Any:
    """Return the first non-empty key value from a record."""
    for key in keys:
        value = record.get(key)
        if value is not None and value != "":
            return value
    return None


def first_dataframe_record(dataframe: Any) -> dict[str, Any]:
    """Convert a DataFrame-like object to its first record dictionary."""
    if dataframe is None or not hasattr(dataframe, "empty") or dataframe.empty:
        return {}
    records = dataframe.to_dict("records")
    if not records:
        return {}
    first = records[0]
    return first if isinstance(first, dict) else {}


def _pick_fy1_estimate(
    estimates: Any,
    last_reported_fiscal_date: str | None = None,
) -> dict[str, Any] | None:
    """Pick the first analyst estimate row after the last reported fiscal date."""
    parsed_cutoff: date
    if last_reported_fiscal_date:
        try:
            parsed_cutoff = date.fromisoformat(str(last_reported_fiscal_date)[:10])
        except ValueError:
            parsed_cutoff = date.today()
    else:
        parsed_cutoff = date.today()

    if isinstance(estimates, dict):
        estimate_rows = [estimates]
    elif isinstance(estimates, list):
        estimate_rows = [row for row in estimates if isinstance(row, dict)]
    else:
        estimate_rows = []

    dated_rows: list[tuple[date, dict[str, Any]]] = []
    for row in estimate_rows:
        raw_date = row.get("date")
        if not raw_date:
            continue
        try:
            fiscal_date = date.fromisoformat(str(raw_date)[:10])
        except ValueError:
            continue
        dated_rows.append((fiscal_date, row))

    for fiscal_date, row in sorted(dated_rows, key=lambda item: item[0]):
        if fiscal_date <= parsed_cutoff:
            continue
        return row

    return None


def compute_forward_pe(
    current_price: Any,
    estimates: Any,
    last_reported_fiscal_date: str | None = None,
) -> dict[str, Any]:
    """Compute FY1 forward P/E from analyst estimates."""
    result = {
        "forward_pe": None,
        "ntm_eps": None,
        "pe_source": "unavailable",
        "analyst_count": None,
        "fiscal_period": None,
    }

    price = parse_fmp_float(current_price)
    if price is None or price <= 0:
        return result

    fy1 = _pick_fy1_estimate(estimates, last_reported_fiscal_date)
    if fy1 is None:
        return result

    eps_avg = parse_fmp_float(fy1.get("epsAvg"))
    if eps_avg is None or eps_avg <= 0:
        return {
            **result,
            "pe_source": "negative_forward_earnings",
        }

    return {
        "forward_pe": round(price / eps_avg, 2),
        "ntm_eps": eps_avg,
        "pe_source": "forward",
        "analyst_count": fy1.get("numAnalystsEps"),
        "fiscal_period": str(fy1.get("date"))[:10],
    }


def compute_forward_ev_ebitda(
    enterprise_value: Any,
    estimates: Any,
    last_reported_fiscal_date: str | None = None,
) -> float | None:
    """Compute FY1 forward EV/EBITDA from analyst estimates."""
    ev = parse_fmp_float(enterprise_value)
    if ev is None or ev <= 0:
        return None

    fy1 = _pick_fy1_estimate(estimates, last_reported_fiscal_date)
    if fy1 is None:
        return None

    ebitda_avg = parse_fmp_float(fy1.get("ebitdaAvg"))
    if ebitda_avg is None or ebitda_avg <= 0:
        return None

    return round(ev / ebitda_avg, 2)


def compute_forward_ev_sales(
    enterprise_value: Any,
    estimates: Any,
    last_reported_fiscal_date: str | None = None,
) -> float | None:
    """Compute FY1 forward EV/Sales from analyst revenue estimates."""
    ev = parse_fmp_float(enterprise_value)
    if ev is None or ev <= 0:
        return None

    fy1 = _pick_fy1_estimate(estimates, last_reported_fiscal_date)
    if fy1 is None:
        return None

    revenue_avg = parse_fmp_float(fy1.get("revenueAvg"))
    if revenue_avg is None or revenue_avg <= 0:
        return None

    return round(ev / revenue_avg, 2)


def _get_last_reported_fiscal_date(fmp_client: Any, ticker: str) -> str | None:
    """Return the most recent reported fiscal period end date for a ticker."""
    try:
        income_df = fmp_client.fetch(
            "income_statement",
            symbol=ticker,
            period="quarter",
            limit=1,
        )
        income_record = first_dataframe_record(income_df)
        raw_date = income_record.get("date")
        return str(raw_date)[:10] if raw_date else None
    except Exception:
        return None


def fetch_forward_pe(fmp_client: Any, ticker: str, current_price: Any) -> dict[str, Any]:
    """Fetch analyst data and compute forward P/E with graceful fallback."""
    fallback = {
        "forward_pe": None,
        "ntm_eps": None,
        "pe_source": "unavailable",
        "analyst_count": None,
        "fiscal_period": None,
    }

    try:
        last_reported_fiscal_date = _get_last_reported_fiscal_date(fmp_client, ticker)
        estimates_df = fmp_client.fetch(
            "analyst_estimates",
            symbol=ticker,
            period="annual",
            limit=4,
        )
        if estimates_df is None:
            estimate_records: list[dict[str, Any]] = []
        elif hasattr(estimates_df, "empty"):
            estimate_records = (
                estimates_df.to_dict("records")
                if not estimates_df.empty
                else []
            )
        elif isinstance(estimates_df, list):
            estimate_records = [row for row in estimates_df if isinstance(row, dict)]
        elif isinstance(estimates_df, dict):
            estimate_records = [estimates_df]
        else:
            estimate_records = []

        return compute_forward_pe(
            current_price,
            estimate_records,
            last_reported_fiscal_date=last_reported_fiscal_date,
        )
    except Exception:
        return fallback
