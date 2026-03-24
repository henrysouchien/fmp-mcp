"""Three-state quality signals derived from FMP financial statements."""

from __future__ import annotations

from typing import Any


def _get_float(record: dict[str, Any], key: str) -> float | None:
    """Safely coerce a record value to float."""
    value = record.get(key)
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def revenue_growth(income: list[dict[str, Any]]) -> bool | None:
    """Return whether revenue grew year over year."""
    if len(income) < 2:
        return None

    revenue_y0 = _get_float(income[0], "revenue")
    revenue_y1 = _get_float(income[1], "revenue")
    if revenue_y0 is None or revenue_y1 is None:
        return None

    return revenue_y0 > revenue_y1 > 0


def positive_fcf(cashflow: list[dict[str, Any]]) -> bool | None:
    """Return whether the last two years had positive free cash flow."""
    if len(cashflow) < 2:
        return None

    fcf_y0 = _get_float(cashflow[0], "freeCashFlow")
    fcf_y1 = _get_float(cashflow[1], "freeCashFlow")
    if fcf_y0 is None or fcf_y1 is None:
        return None

    return fcf_y0 > 0 and fcf_y1 > 0


def capex_increase(cashflow: list[dict[str, Any]]) -> bool | None:
    """Return whether absolute capital expenditure increased year over year."""
    if len(cashflow) < 2:
        return None

    capex_y0 = _get_float(cashflow[0], "capitalExpenditure")
    capex_y1 = _get_float(cashflow[1], "capitalExpenditure")
    if capex_y0 is None or capex_y1 is None:
        return None

    return abs(capex_y0) > abs(capex_y1)


def gross_margin_improvement(income: list[dict[str, Any]]) -> bool | None:
    """Return whether gross margin improved across the last three periods."""
    if len(income) < 3:
        return None

    try:
        margins: list[float] = []
        for index in range(3):
            gross_profit = _get_float(income[index], "grossProfit")
            revenue = _get_float(income[index], "revenue")
            if gross_profit is None or revenue is None or revenue == 0:
                return None
            margins.append(gross_profit / revenue)

        return margins[0] > margins[1] > margins[2]
    except Exception:
        return None


def roe_roic_positive(metrics_ttm: list[dict[str, Any]]) -> bool | None:
    """Return whether ROE or ROIC is positive on a TTM basis."""
    if not metrics_ttm:
        return None

    metrics = metrics_ttm[0]
    roe = _get_float(metrics, "returnOnEquityTTM")
    roic = _get_float(metrics, "returnOnInvestedCapitalTTM")
    if roe is None and roic is None:
        return None

    return (roe is not None and roe > 0) or (roic is not None and roic > 0)


def low_leverage(metrics_ttm: list[dict[str, Any]], threshold: float = 2.0) -> bool | None:
    """Return whether net debt to EBITDA is below the leverage threshold."""
    if not metrics_ttm:
        return None

    net_debt_to_ebitda = _get_float(metrics_ttm[0], "netDebtToEBITDATTM")
    if net_debt_to_ebitda is None:
        return None

    return net_debt_to_ebitda < threshold


SIGNAL_FUNCS = {
    "revenue_growth": lambda income, cashflow, metrics: revenue_growth(income),
    "positive_fcf": lambda income, cashflow, metrics: positive_fcf(cashflow),
    "capex_increase": lambda income, cashflow, metrics: capex_increase(cashflow),
    "gross_margin_improvement": lambda income, cashflow, metrics: gross_margin_improvement(income),
    "roe_roic_positive": lambda income, cashflow, metrics: roe_roic_positive(metrics),
    "low_leverage": lambda income, cashflow, metrics: low_leverage(metrics),
}


def compute_quality_signals(
    income_statements: list[dict[str, Any]],
    cashflow_statements: list[dict[str, Any]],
    metrics_ttm: list[dict[str, Any]],
) -> dict[str, Any]:
    """Compute the 6 quality signals and aggregate score metadata."""
    income = sorted(income_statements, key=lambda record: record.get("date", ""), reverse=True)
    cashflow = sorted(cashflow_statements, key=lambda record: record.get("date", ""), reverse=True)

    signals: dict[str, bool | None] = {}
    for name, func in SIGNAL_FUNCS.items():
        signals[name] = func(income, cashflow, metrics_ttm)

    passing = sum(1 for value in signals.values() if value is True)
    failing = sum(1 for value in signals.values() if value is False)
    evaluated = passing + failing

    return {
        "quality": {
            "signals": signals,
            "score": passing,
            "evaluated": evaluated,
            "max_signals": len(SIGNAL_FUNCS),
        }
    }
