"""Shared helpers for normalizing FMP payload values."""

from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN, localcontext
from typing import Any, Optional

from value_semantics_core import (
    ForwardEpsEstimate,
    ForwardPeAvailable,
    Fy1ForwardPeriodAvailable,
    derive_fy1_forward_pe,
    select_fy1_forward_period,
)


_FORWARD_PE_QUANTUM = Decimal("0.01")


def parse_fmp_decimal(value: Any) -> Decimal | None:
    """Convert an FMP numeric payload value into a finite ``Decimal``."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        wrapped_negative = text.startswith("(") and text.endswith(")")
        text = text.replace("%", "").replace(",", "").replace("(", "").replace(")", "")
        if text.startswith("+"):
            text = text[1:]
        try:
            converted = Decimal(text)
        except InvalidOperation:
            return None
        if wrapped_negative and converted > 0:
            converted = -converted
    elif isinstance(value, (int, float, Decimal)):
        try:
            converted = Decimal(str(value))
        except InvalidOperation:
            return None
    else:
        return None
    return converted if converted.is_finite() else None


def parse_fmp_float(value: Any) -> Optional[float]:
    """Convert FMP numeric payload values into finite float values."""
    converted = parse_fmp_decimal(value)
    return float(converted) if converted is not None else None


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


def compute_forward_pe(
    current_price: Any,
    estimates: Any,
    last_reported_fiscal_date: str | None = None,
) -> dict[str, Any]:
    """Compute FY1 forward P/E from analyst estimates."""
    result = {
        "forward_pe": None,
        "fy1_eps": None,
        "forward_pe_basis": "fy1",
        "pe_source": "unavailable",
        "analyst_count": None,
        "fiscal_period": None,
    }

    try:
        fiscal_anchor = (
            date.fromisoformat(str(last_reported_fiscal_date)[:10])
            if last_reported_fiscal_date
            else None
        )
    except ValueError:
        fiscal_anchor = None

    if isinstance(estimates, dict):
        estimate_rows = [estimates]
    elif isinstance(estimates, list):
        estimate_rows = [row for row in estimates if isinstance(row, dict)]
    else:
        estimate_rows = []

    normalized_estimates: list[ForwardEpsEstimate] = []
    for row in estimate_rows:
        raw_period_end = row.get("date")
        if not raw_period_end:
            continue
        try:
            period_end = date.fromisoformat(str(raw_period_end)[:10])
        except ValueError:
            continue
        eps = parse_fmp_decimal(row.get("epsAvg"))
        if eps is None:
            continue
        analyst_count_decimal = parse_fmp_decimal(row.get("numAnalystsEps"))
        analyst_count = (
            int(analyst_count_decimal)
            if analyst_count_decimal is not None
            and analyst_count_decimal >= 0
            and analyst_count_decimal == analyst_count_decimal.to_integral_value()
            else None
        )
        normalized_estimates.append(
            ForwardEpsEstimate(
                period_end=period_end,
                eps=eps,
                analyst_count=analyst_count,
            )
        )

    derived = derive_fy1_forward_pe(
        price=parse_fmp_decimal(current_price),
        estimates=normalized_estimates,
        last_reported_period_end=fiscal_anchor,
        quantum=_FORWARD_PE_QUANTUM,
    )
    if isinstance(derived, ForwardPeAvailable):
        return {
            "forward_pe": float(derived.multiple),
            "fy1_eps": float(derived.eps),
            "forward_pe_basis": "fy1",
            "pe_source": "forward",
            "analyst_count": derived.analyst_count,
            "fiscal_period": derived.estimate_period_end.isoformat(),
        }

    fiscal_period = (
        derived.estimate_period_end.isoformat()
        if derived.estimate_period_end is not None
        else None
    )
    return {
        **result,
        "fy1_eps": float(derived.eps) if derived.eps is not None else None,
        "pe_source": (
            "negative_forward_earnings"
            if derived.reason == "non_positive_eps"
            else "unavailable"
        ),
        "fiscal_period": fiscal_period,
    }


def compute_forward_ev_sales(
    enterprise_value: Any,
    estimates: Any,
    last_reported_fiscal_date: str | None = None,
) -> float | None:
    """Compute FY1 forward EV/Sales from analyst revenue estimates."""
    ev = parse_fmp_decimal(enterprise_value)
    if ev is None or ev <= 0 or not last_reported_fiscal_date:
        return None
    try:
        fiscal_anchor = date.fromisoformat(str(last_reported_fiscal_date)[:10])
    except ValueError:
        return None
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
            period_end = date.fromisoformat(str(raw_date)[:10])
        except ValueError:
            continue
        dated_rows.append((period_end, row))
    selection = select_fy1_forward_period(
        period_ends=tuple(period_end for period_end, _row in dated_rows),
        last_reported_period_end=fiscal_anchor,
    )
    if not isinstance(selection, Fy1ForwardPeriodAvailable):
        return None
    fy1 = next(
        row for period_end, row in dated_rows if period_end == selection.period_end
    )
    revenue_avg = parse_fmp_decimal(fy1.get("revenueAvg"))
    if revenue_avg is None or revenue_avg <= 0:
        return None
    with localcontext() as context:
        context.prec = max(
            28,
            len(ev.as_tuple().digits) + len(revenue_avg.as_tuple().digits) + 8,
        )
        multiple = (ev / revenue_avg).quantize(
            Decimal("0.01"),
            rounding=ROUND_HALF_EVEN,
        )
    return float(multiple)


def _get_last_reported_fiscal_date(fmp_client: Any, ticker: str) -> str | None:
    """Return the most recent reported fiscal period end date for a ticker."""
    income_df = fmp_client.fetch(
        "income_statement",
        symbol=ticker,
        period="quarter",
        limit=1,
    )
    income_record = first_dataframe_record(income_df)
    raw_date = income_record.get("date")
    return str(raw_date)[:10] if raw_date else None


def fetch_forward_pe(
    fmp_client: Any, ticker: str, current_price: Any
) -> dict[str, Any]:
    """Fetch analyst data and compute forward P/E."""
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
            estimates_df.to_dict("records") if not estimates_df.empty else []
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
