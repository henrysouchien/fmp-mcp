"""
MCP Tool: get_stock_fundamentals

Enriched fundamentals and pricing lookup for a single stock symbol.
"""

from __future__ import annotations

import math
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Any, Literal

from ..client import FMPClient
from ..exceptions import FMPEmptyResponseError
from ..quality_signals import compute_quality_signals
from ._helpers import _last_trading_day
from .technical import get_technical_analysis
from utils.fmp_helpers import (
    _get_last_reported_fiscal_date,
    compute_forward_ev_ebitda,
    compute_forward_ev_sales,
    compute_forward_pe,
    parse_fmp_float,
)


VALID_SECTIONS = [
    "profile",
    "quote",
    "financials",
    "valuation",
    "profitability",
    "balance_sheet",
    "quality",
    "technicals",
    "chart",
]
SUMMARY_SECTIONS = VALID_SECTIONS[:-1]


def _pick_value(record: dict[str, Any], *keys: str) -> Any:
    """Return the first non-empty value for the candidate keys."""
    for key in keys:
        value = record.get(key)
        if value is not None and value != "":
            return value
    return None


def _records_from_payload(payload: Any) -> list[dict[str, Any]]:
    """Normalize raw endpoint payloads into a list of plain dict records."""
    if payload is None:
        return []

    if hasattr(payload, "to_dict"):
        try:
            records = payload.to_dict("records")
        except TypeError:
            records = payload.to_dict()
        if isinstance(records, list):
            return [record for record in records if isinstance(record, dict)]
        if isinstance(records, dict):
            return [records] if records else []

    if isinstance(payload, dict):
        historical = payload.get("historical")
        if isinstance(historical, list):
            return [record for record in historical if isinstance(record, dict)]
        return [payload] if payload else []

    if isinstance(payload, list):
        return [record for record in payload if isinstance(record, dict)]

    return []


def _first_record(payload: Any) -> dict[str, Any]:
    """Return the first normalized record from a payload."""
    records = _records_from_payload(payload)
    return records[0] if records else {}


def _parse_metric(record: dict[str, Any], *keys: str) -> float | None:
    """Parse the first matching numeric field from a record."""
    value = parse_fmp_float(_pick_value(record, *keys))
    if value is None or not math.isfinite(value):
        return None
    return value


def _parse_int_like_metric(record: dict[str, Any], *keys: str) -> int | float | None:
    """Parse an integer-like numeric field while preserving fractional values."""
    value = _parse_metric(record, *keys)
    if value is None:
        return None
    return int(value) if value.is_integer() else value


def _fetch_raw_task(
    client: FMPClient,
    key: str,
    endpoint_name: str,
    **params: Any,
) -> tuple[str, Any, str | None]:
    """Fetch one raw FMP payload and capture failures instead of raising."""
    try:
        return key, client.fetch_raw(endpoint_name, **params), None
    except FMPEmptyResponseError as exc:
        return key, None, str(exc)
    except Exception as exc:
        return key, None, str(exc)


def _fetch_last_reported_date_task(
    client: FMPClient,
    symbol: str,
) -> tuple[str, str | None, str | None]:
    """Fetch the most recent reported fiscal period end date."""
    try:
        return "last_reported_fiscal_date", _get_last_reported_fiscal_date(client, symbol), None
    except Exception as exc:
        return "last_reported_fiscal_date", None, str(exc)


def _fetch_technical_task(symbol: str) -> tuple[str, Any, str | None]:
    """Fetch technical-analysis summary using the internal MCP helper."""
    try:
        result = get_technical_analysis(
            symbol,
            indicators=["rsi", "macd", "bollinger"],
            format="summary",
        )
        return "technicals", result, None
    except Exception as exc:
        return "technicals", {"status": "error", "error": str(exc)}, str(exc)


def _fetch_sector_pe(client: FMPClient, sector: str) -> tuple[float | None, str | None]:
    """Fetch sector average P/E for the matching sector when available."""
    try:
        payload = client.fetch_raw("sector_pe_snapshot", date=_last_trading_day())
    except FMPEmptyResponseError as exc:
        return None, str(exc)
    except Exception as exc:
        return None, str(exc)

    sector_lower = sector.strip().lower()
    for row in _records_from_payload(payload):
        row_sector = str(row.get("sector") or "").strip().lower()
        if row_sector != sector_lower:
            continue

        pe_value = parse_fmp_float(_pick_value(row, "pe", "peRatio"))
        if pe_value is not None and math.isfinite(pe_value) and pe_value > 0:
            return pe_value, None
        return None, None

    return None, None


def _build_profile(profile_data: Any) -> tuple[dict[str, Any] | None, list[str]]:
    """Build the profile section."""
    try:
        profile = _first_record(profile_data)
        if not profile:
            return None, ["profile: no usable profile data returned."]

        section: dict[str, Any] = {}

        company_name = _pick_value(profile, "companyName", "name")
        if company_name is not None:
            section["company_name"] = str(company_name)

        sector = _pick_value(profile, "sector")
        if sector is not None:
            section["sector"] = str(sector)

        industry = _pick_value(profile, "industry")
        if industry is not None:
            section["industry"] = str(industry)

        exchange = _pick_value(profile, "exchangeShortName", "exchange", "exchangeName")
        if exchange is not None:
            section["exchange"] = str(exchange)

        if not section:
            return None, ["profile: no usable profile fields returned."]

        return section, []
    except Exception as exc:
        return None, [f"profile: failed to build section: {exc}"]


def _build_quote(quote_data: Any) -> tuple[dict[str, Any] | None, list[str]]:
    """Build the quote section."""
    try:
        quote = _first_record(quote_data)
        if not quote:
            return None, ["quote: no usable quote data returned."]

        section: dict[str, Any] = {}

        price = _parse_metric(quote, "price")
        if price is not None:
            section["price"] = price

        change = _parse_metric(quote, "change")
        if change is not None:
            section["change"] = change

        change_percent = _parse_metric(quote, "changesPercentage", "changePercent")
        if change_percent is not None:
            section["change_percent"] = change_percent

        market_cap = _parse_int_like_metric(quote, "marketCap")
        if market_cap is not None:
            section["market_cap"] = market_cap

        volume = _parse_int_like_metric(quote, "volume")
        if volume is not None:
            section["volume"] = volume

        eps = _parse_metric(quote, "eps")
        if eps is not None:
            section["eps"] = eps

        if not section:
            return None, ["quote: no usable quote fields returned."]

        return section, []
    except Exception as exc:
        return None, [f"quote: failed to build section: {exc}"]


def _build_financials(
    quarterly_income: Any,
    quarterly_cash_flow: Any,
) -> tuple[dict[str, Any] | None, list[str]]:
    """Build the financials section with TTM absolute figures."""
    try:
        income_rows = _records_from_payload(quarterly_income)
        cash_flow_rows = _records_from_payload(quarterly_cash_flow)

        section: dict[str, Any] = {}

        def _ttm_sum(rows: list[dict[str, Any]], field: str) -> float | None:
            """Sum 4 parsed quarterly values. Returns None if fewer than 4 valid."""
            values = []
            for row in rows:
                parsed = parse_fmp_float(row.get(field))
                if parsed is not None and math.isfinite(parsed):
                    values.append(parsed)
            return sum(values) if len(values) == 4 else None

        rev = _ttm_sum(income_rows, "revenue")
        if rev is not None:
            section["revenue_ttm"] = rev

        ebitda = _ttm_sum(income_rows, "ebitda")
        if ebitda is not None:
            section["ebitda_ttm"] = ebitda

        ni = _ttm_sum(income_rows, "netIncome")
        if ni is not None:
            section["net_income_ttm"] = ni

        fcf = _ttm_sum(cash_flow_rows, "freeCashFlow")
        if fcf is not None:
            section["free_cash_flow_ttm"] = fcf

        ocf = _ttm_sum(cash_flow_rows, "operatingCashFlow")
        if ocf is not None:
            section["operating_cash_flow_ttm"] = ocf

        capex = _ttm_sum(cash_flow_rows, "capitalExpenditure")
        if capex is not None:
            section["capex_ttm"] = capex

        if not section:
            return None, ["financials: no usable quarterly data returned."]

        if income_rows:
            currency = income_rows[0].get("reportedCurrency")
            if currency:
                section["reported_currency"] = currency

        return section, []
    except Exception as exc:
        return None, [f"financials: failed to build section: {exc}"]


def _build_valuation(
    ratios: Any,
    key_metrics: Any,
    forward_pe_result: dict[str, Any],
    sector_pe: float | None,
    forward_ev_ebitda: float | None = None,
    forward_ev_sales: float | None = None,
) -> tuple[dict[str, Any] | None, list[str]]:
    """Build the valuation section."""
    try:
        ratios_row = _first_record(ratios)
        key_metrics_row = _first_record(key_metrics)

        if not ratios_row:
            return None, ["valuation: no usable ratios data returned."]

        section: dict[str, Any] = {}

        forward_pe = parse_fmp_float(forward_pe_result.get("forward_pe"))
        if forward_pe is not None and math.isfinite(forward_pe):
            section["forward_pe"] = forward_pe

        pe_ratio_ttm = _parse_metric(
            ratios_row,
            "priceToEarningsRatioTTM",
            "peRatioTTM",
            "priceEarningsRatioTTM",
            "peRatio",
        )
        if pe_ratio_ttm is not None:
            section["pe_ratio_ttm"] = pe_ratio_ttm

        ntm_eps = parse_fmp_float(forward_pe_result.get("ntm_eps"))
        if ntm_eps is not None and math.isfinite(ntm_eps):
            section["ntm_eps"] = ntm_eps

        analyst_count = forward_pe_result.get("analyst_count")
        if analyst_count is not None:
            section["analyst_count_eps"] = analyst_count

        pb_ratio = _parse_metric(
            ratios_row,
            "priceToBookRatioTTM",
            "pbRatioTTM",
            "pbRatio",
        )
        if pb_ratio is not None:
            section["pb_ratio"] = pb_ratio

        price_to_fcf = _parse_metric(ratios_row, "priceToFreeCashFlowRatioTTM")
        if price_to_fcf is not None:
            section["price_to_fcf"] = price_to_fcf

        forward_peg = _parse_metric(ratios_row, "forwardPriceToEarningsGrowthRatioTTM")
        if forward_peg is None and key_metrics_row:
            forward_peg = _parse_metric(key_metrics_row, "forwardPriceToEarningsGrowthRatioTTM")
        if forward_peg is not None:
            section["peg_ratio"] = forward_peg
            section["peg_source"] = "FY1"
        else:
            ttm_peg = _parse_metric(
                ratios_row,
                "priceToEarningsGrowthRatioTTM",
                "pegRatioTTM",
                "pegRatio",
            )
            if ttm_peg is None and key_metrics_row:
                ttm_peg = _parse_metric(
                    key_metrics_row,
                    "priceToEarningsGrowthRatioTTM",
                    "pegRatioTTM",
                    "pegRatio",
                )
            if ttm_peg is not None:
                section["peg_ratio"] = ttm_peg
                section["peg_source"] = "ttm"

        if forward_ev_ebitda is not None:
            section["ev_ebitda"] = forward_ev_ebitda
            section["ev_ebitda_source"] = "FY1"
        else:
            ev_ebitda_ttm = _parse_metric(
                ratios_row,
                "enterpriseValueMultipleTTM",
                "evToEbitdaTTM",
            )
            if ev_ebitda_ttm is None and key_metrics_row:
                ev_ebitda_ttm = _parse_metric(
                    key_metrics_row,
                    "enterpriseValueMultipleTTM",
                    "evToEbitdaTTM",
                )
            if ev_ebitda_ttm is not None:
                section["ev_ebitda"] = ev_ebitda_ttm
                section["ev_ebitda_source"] = "ttm"

        if forward_ev_sales is not None:
            section["ev_sales"] = forward_ev_sales
            section["ev_sales_source"] = "FY1"
        else:
            ev_sales_ttm = _parse_metric(ratios_row, "priceToSalesRatioTTM")
            if ev_sales_ttm is not None:
                section["ev_sales"] = ev_sales_ttm
                section["ev_sales_source"] = "ttm"

        dividend_yield = _parse_metric(
            ratios_row,
            "dividendYielTTM",
            "dividendYieldTTM",
            "dividendYielPercentageTTM",
            "dividendYieldPercentageTTM",
            "dividendYield",
        )
        if dividend_yield is not None:
            section["dividend_yield"] = dividend_yield

        if sector_pe is not None:
            section["sector_avg_pe"] = sector_pe

        if forward_pe is not None:
            section["pe_source"] = "forward"
        elif pe_ratio_ttm is not None:
            section["pe_source"] = "ttm"
        else:
            section["pe_source"] = str(forward_pe_result.get("pe_source") or "unavailable")

        if not section:
            return None, ["valuation: no usable valuation fields returned."]

        return section, []
    except Exception as exc:
        return None, [f"valuation: failed to build section: {exc}"]


def _build_profitability(
    ratios: Any,
    key_metrics: Any,
) -> tuple[dict[str, Any] | None, list[str]]:
    """Build the profitability section."""
    try:
        ratios_row = _first_record(ratios)
        key_metrics_row = _first_record(key_metrics)

        if not ratios_row:
            return None, ["profitability: no usable ratios data returned."]

        section: dict[str, Any] = {}

        roe = _parse_metric(ratios_row, "returnOnEquityTTM", "roeTTM", "returnOnEquity")
        if roe is not None:
            section["roe"] = roe

        roic = _parse_metric(key_metrics_row, "returnOnInvestedCapitalTTM")
        if roic is not None:
            section["roic"] = roic

        gross_margin = _parse_metric(
            ratios_row,
            "grossProfitMarginTTM",
            "grossMarginTTM",
            "grossProfitMargin",
        )
        if gross_margin is not None:
            section["gross_margin"] = gross_margin

        operating_margin = _parse_metric(
            ratios_row,
            "operatingProfitMarginTTM",
            "operatingMarginTTM",
            "operatingProfitMargin",
        )
        if operating_margin is not None:
            section["operating_margin"] = operating_margin

        net_profit_margin = _parse_metric(
            ratios_row,
            "netProfitMarginTTM",
            "profitMarginTTM",
            "netProfitMargin",
        )
        if net_profit_margin is not None:
            section["net_profit_margin"] = net_profit_margin

        if not section:
            return None, ["profitability: no usable profitability fields returned."]

        return section, []
    except Exception as exc:
        return None, [f"profitability: failed to build section: {exc}"]


def _build_balance_sheet(
    ratios: Any,
    key_metrics: Any,
) -> tuple[dict[str, Any] | None, list[str]]:
    """Build the balance-sheet section."""
    try:
        ratios_row = _first_record(ratios)
        key_metrics_row = _first_record(key_metrics)

        if not ratios_row:
            return None, ["balance_sheet: no usable ratios data returned."]

        section: dict[str, Any] = {}

        debt_to_equity = _parse_metric(
            ratios_row,
            "debtToEquityRatioTTM",
            "debtEquityRatioTTM",
            "debtToEquityTTM",
            "debtToEquity",
        )
        if debt_to_equity is not None:
            section["debt_to_equity"] = debt_to_equity

        current_ratio = _parse_metric(ratios_row, "currentRatioTTM", "currentRatio")
        if current_ratio is not None:
            section["current_ratio"] = current_ratio

        net_debt_to_ebitda = _parse_metric(key_metrics_row, "netDebtToEBITDATTM")
        if net_debt_to_ebitda is not None:
            section["net_debt_to_ebitda"] = net_debt_to_ebitda

        if not section:
            return None, ["balance_sheet: no usable leverage fields returned."]

        return section, []
    except Exception as exc:
        return None, [f"balance_sheet: failed to build section: {exc}"]


def _build_quality(
    income_stmts: Any,
    cashflow_stmts: Any,
    metrics_ttm: Any,
) -> tuple[dict[str, Any] | None, list[str]]:
    """Build the quality section."""
    try:
        income_records = _records_from_payload(income_stmts)
        cashflow_records = _records_from_payload(cashflow_stmts)
        metrics_records = _records_from_payload(metrics_ttm)

        if not income_records and not cashflow_records and not metrics_records:
            return None, ["quality: no usable financial statement data returned."]

        quality = compute_quality_signals(
            income_statements=income_records,
            cashflow_statements=cashflow_records,
            metrics_ttm=metrics_records,
        ).get("quality")

        if not isinstance(quality, dict) or not quality:
            return None, ["quality: failed to compute quality signals."]

        return quality, []
    except Exception as exc:
        return None, [f"quality: failed to build section: {exc}"]


def _build_technicals(tech_result: Any) -> tuple[dict[str, Any] | None, list[str]]:
    """Build the technicals section."""
    try:
        if not isinstance(tech_result, dict) or tech_result.get("status") != "success":
            error = None
            if isinstance(tech_result, dict):
                error = tech_result.get("error")
            suffix = f": {error}" if error else ""
            return None, [f"technicals: technical analysis failed{suffix}"]

        section: dict[str, Any] = {}

        momentum = tech_result.get("momentum", {})
        if isinstance(momentum, dict):
            rsi_data = momentum.get("rsi")
            if isinstance(rsi_data, dict):
                rsi_value = parse_fmp_float(rsi_data.get("value"))
                if rsi_value is not None and math.isfinite(rsi_value):
                    section["rsi"] = rsi_value
                rsi_signal = rsi_data.get("signal")
                if rsi_signal:
                    section["rsi_signal"] = str(rsi_signal)

            macd_data = momentum.get("macd")
            if isinstance(macd_data, dict):
                macd_signal = macd_data.get("signal")
                if macd_signal:
                    section["macd_signal"] = str(macd_signal)

        volatility = tech_result.get("volatility", {})
        if isinstance(volatility, dict):
            bollinger = volatility.get("bollinger")
            if isinstance(bollinger, dict):
                bollinger_signal = str(bollinger.get("signal") or "").lower()
                if bollinger_signal in {"near_upper_band", "above_upper_band"}:
                    section["bollinger_position"] = "Upper"
                elif bollinger_signal in {"near_lower_band", "below_lower_band"}:
                    section["bollinger_position"] = "Lower"
                elif bollinger_signal:
                    section["bollinger_position"] = "Middle"

        composite_signal = tech_result.get("composite_signal")
        if composite_signal:
            section["composite_signal"] = str(composite_signal)

        support_resistance = tech_result.get("support_resistance", {})
        if isinstance(support_resistance, dict):
            supports = support_resistance.get("support")
            if isinstance(supports, list) and supports:
                support_price = parse_fmp_float(supports[0].get("price"))
                if support_price is not None and math.isfinite(support_price):
                    section["support"] = support_price

            resistances = support_resistance.get("resistance")
            if isinstance(resistances, list) and resistances:
                resistance_price = parse_fmp_float(resistances[0].get("price"))
                if resistance_price is not None and math.isfinite(resistance_price):
                    section["resistance"] = resistance_price

        if not section:
            return None, ["technicals: no usable technical fields returned."]

        return section, []
    except Exception as exc:
        return None, [f"technicals: failed to build section: {exc}"]


def _build_chart(chart_data: Any) -> tuple[list[dict[str, Any]] | None, list[str]]:
    """Build the chart section."""
    try:
        rows = _records_from_payload(chart_data)
        if not rows:
            return None, ["chart: no usable historical price data returned."]

        chart_rows: list[dict[str, Any]] = []
        for row in sorted(rows, key=lambda record: str(record.get("date", ""))):
            date_value = row.get("date")
            price_value = parse_fmp_float(_pick_value(row, "adjClose", "close", "price"))
            volume_value = parse_fmp_float(row.get("volume"))

            if date_value is None or price_value is None or not math.isfinite(price_value):
                continue

            if volume_value is None or not math.isfinite(volume_value):
                volume_output: int | float = 0
            else:
                volume_output = int(volume_value) if volume_value.is_integer() else volume_value

            chart_rows.append(
                {
                    "date": str(date_value)[:10],
                    "price": price_value,
                    "volume": volume_output,
                }
            )

        if not chart_rows:
            return None, ["chart: no usable chart rows returned."]

        return chart_rows, []
    except Exception as exc:
        return None, [f"chart: failed to build section: {exc}"]


def _normalize_include(
    include: list[str] | None,
    format: Literal["full", "summary"],
) -> tuple[list[str], list[str]]:
    """Normalize requested sections and track invalid section names."""
    default_sections = VALID_SECTIONS if format == "full" else SUMMARY_SECTIONS
    if include is None:
        return list(default_sections), []

    requested_lookup: set[str] = set()
    invalid: list[str] = []

    for raw_name in include:
        name = str(raw_name or "").strip().lower()
        if not name:
            continue
        if name == "chart" and format != "full":
            continue
        if name not in VALID_SECTIONS:
            if name not in invalid:
                invalid.append(name)
            continue
        requested_lookup.add(name)

    normalized = [section for section in VALID_SECTIONS if section in requested_lookup]
    return normalized, invalid


def get_stock_fundamentals(
    symbol: str,
    include: list[str] | None = None,
    format: Literal["full", "summary"] = "summary",
) -> dict:
    """
    Get enriched stock fundamentals, pricing, quality, and technical context.

    Args:
        symbol: Stock symbol (e.g., "AAPL").
        include: Optional list of sections to include. Valid sections:
            "profile", "quote", "financials", "valuation",
            "profitability", "balance_sheet", "quality", "technicals",
            and "chart" (full format only). Defaults to all sections
            supported for the chosen format.
        format: "summary" for compact fundamentals, "full" to also include
            2-year daily chart data.

    Returns:
        dict with status field ("success" or "error"), normalized section data,
        `sections_included`, `sections_failed`, and warnings for any partial
        failures.
    """
    _saved = sys.stdout
    sys.stdout = sys.stderr
    try:
        normalized_symbol = str(symbol or "").upper().strip()
        if not normalized_symbol:
            return {
                "status": "error",
                "error": "symbol is required. Specify a stock symbol (e.g., 'AAPL').",
            }

        requested_sections, invalid_sections = _normalize_include(include, format)

        response: dict[str, Any] = {
            "status": "success",
            "symbol": normalized_symbol,
            "as_of": date.today().isoformat(),
            "sections_included": [],
            "sections_failed": [],
            "warnings": [],
        }

        if invalid_sections:
            response["warnings"].append(
                "Ignored unrecognized include sections: "
                + ", ".join(invalid_sections)
                + "."
            )

        if not requested_sections:
            return response

        client = FMPClient()
        raw_results: dict[str, Any] = {}
        fetch_errors: dict[str, str] = {}

        needs_profile = "profile" in requested_sections or "valuation" in requested_sections
        needs_quote = "quote" in requested_sections or "valuation" in requested_sections
        needs_financials = "financials" in requested_sections
        needs_ratios = any(
            section in requested_sections
            for section in ("valuation", "profitability", "balance_sheet")
        )
        needs_key_metrics = any(
            section in requested_sections
            for section in ("valuation", "profitability", "balance_sheet", "quality")
        )
        needs_income = "quality" in requested_sections
        needs_cash_flow = "quality" in requested_sections
        needs_forward_pe = "valuation" in requested_sections
        needs_technicals = "technicals" in requested_sections
        needs_chart = format == "full" and "chart" in requested_sections

        fetch_specs: list[tuple[str, str, dict[str, Any]]] = []
        if needs_profile:
            fetch_specs.append(("profile", "profile", {"symbol": normalized_symbol}))
        if needs_quote:
            fetch_specs.append(("quote", "quote", {"symbol": normalized_symbol}))
        if needs_ratios:
            fetch_specs.append(("ratios", "ratios_ttm", {"symbol": normalized_symbol}))
        if needs_key_metrics:
            fetch_specs.append(("key_metrics", "key_metrics_ttm", {"symbol": normalized_symbol}))
        if needs_income:
            fetch_specs.append(
                (
                    "income_statements",
                    "income_statement",
                    {"symbol": normalized_symbol, "limit": 3, "period": "annual"},
                )
            )
        if needs_cash_flow:
            fetch_specs.append(
                (
                    "cash_flow_statements",
                    "cash_flow",
                    {"symbol": normalized_symbol, "limit": 3, "period": "annual"},
                )
            )
        if needs_financials:
            fetch_specs.append(
                (
                    "quarterly_income",
                    "income_statement",
                    {"symbol": normalized_symbol, "limit": 4, "period": "quarter"},
                )
            )
            fetch_specs.append(
                (
                    "quarterly_cash_flow",
                    "cash_flow",
                    {"symbol": normalized_symbol, "limit": 4, "period": "quarter"},
                )
            )
        if needs_forward_pe:
            fetch_specs.append(
                (
                    "analyst_estimates",
                    "analyst_estimates",
                    {"symbol": normalized_symbol, "period": "annual", "limit": 4},
                )
            )
        if needs_chart:
            today = date.today()
            fetch_specs.append(
                (
                    "chart",
                    "historical_price_adjusted",
                    {
                        "symbol": normalized_symbol,
                        "from_date": (today - timedelta(days=730)).isoformat(),
                        "to_date": today.isoformat(),
                    },
                )
            )

        futures = {}
        max_workers = max(1, min(len(fetch_specs) + int(needs_forward_pe) + int(needs_technicals), 8))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for key, endpoint_name, params in fetch_specs:
                future = executor.submit(_fetch_raw_task, client, key, endpoint_name, **params)
                futures[future] = key

            if needs_forward_pe:
                future = executor.submit(_fetch_last_reported_date_task, client, normalized_symbol)
                futures[future] = "last_reported_fiscal_date"

            if needs_technicals:
                future = executor.submit(_fetch_technical_task, normalized_symbol)
                futures[future] = "technicals"

            for future in as_completed(futures):
                key, payload, error = future.result()
                raw_results[key] = payload
                if error:
                    fetch_errors[key] = error

        sector_pe: float | None = None
        if "valuation" in requested_sections:
            profile_row = _first_record(raw_results.get("profile"))
            sector_name = _pick_value(profile_row, "sector")
            if sector_name:
                sector_pe, sector_error = _fetch_sector_pe(client, str(sector_name))
                if sector_error:
                    response["warnings"].append(
                        f"valuation: failed to fetch sector P/E snapshot: {sector_error}"
                    )

        forward_pe_result = {
            "forward_pe": None,
            "ntm_eps": None,
            "pe_source": "unavailable",
            "analyst_count": None,
            "fiscal_period": None,
        }
        if "valuation" in requested_sections:
            quote_row = _first_record(raw_results.get("quote"))
            quote_price = _pick_value(quote_row, "price")
            estimates = _records_from_payload(raw_results.get("analyst_estimates"))
            forward_pe_result = compute_forward_pe(
                quote_price,
                estimates,
                raw_results.get("last_reported_fiscal_date"),
            )

        forward_ev_ebitda: float | None = None
        forward_ev_sales: float | None = None
        if "valuation" in requested_sections:
            ratios_row = _first_record(raw_results.get("ratios"))
            key_metrics_row = _first_record(raw_results.get("key_metrics"))
            enterprise_value = None
            if ratios_row:
                enterprise_value = parse_fmp_float(_pick_value(ratios_row, "enterpriseValueTTM"))
            if enterprise_value is None and key_metrics_row:
                enterprise_value = parse_fmp_float(
                    _pick_value(key_metrics_row, "enterpriseValueTTM")
                )

            last_reported = raw_results.get("last_reported_fiscal_date")
            forward_ev_ebitda = compute_forward_ev_ebitda(
                enterprise_value,
                estimates,
                last_reported,
            )
            forward_ev_sales = compute_forward_ev_sales(
                enterprise_value,
                estimates,
                last_reported,
            )

        for section_name in requested_sections:
            section_warnings: list[str] = []
            section_data: Any = None

            if section_name == "profile":
                if fetch_errors.get("profile"):
                    section_warnings.append(
                        f"profile: failed to fetch profile data: {fetch_errors['profile']}"
                    )
                section_data, builder_warnings = _build_profile(raw_results.get("profile"))
                section_warnings.extend(builder_warnings)

            elif section_name == "quote":
                if fetch_errors.get("quote"):
                    section_warnings.append(
                        f"quote: failed to fetch quote data: {fetch_errors['quote']}"
                    )
                section_data, builder_warnings = _build_quote(raw_results.get("quote"))
                section_warnings.extend(builder_warnings)

            elif section_name == "financials":
                if fetch_errors.get("quarterly_income"):
                    section_warnings.append(
                        "financials: failed to fetch quarterly income: "
                        + fetch_errors["quarterly_income"]
                    )
                if fetch_errors.get("quarterly_cash_flow"):
                    section_warnings.append(
                        "financials: failed to fetch quarterly cash flow: "
                        + fetch_errors["quarterly_cash_flow"]
                    )
                section_data, builder_warnings = _build_financials(
                    raw_results.get("quarterly_income"),
                    raw_results.get("quarterly_cash_flow"),
                )
                section_warnings.extend(builder_warnings)

            elif section_name == "valuation":
                if fetch_errors.get("ratios"):
                    section_warnings.append(
                        f"valuation: failed to fetch ratios data: {fetch_errors['ratios']}"
                    )
                if fetch_errors.get("analyst_estimates"):
                    section_warnings.append(
                        "valuation: failed to fetch analyst estimates: "
                        + fetch_errors["analyst_estimates"]
                    )
                section_data, builder_warnings = _build_valuation(
                    raw_results.get("ratios"),
                    raw_results.get("key_metrics"),
                    forward_pe_result,
                    sector_pe,
                    forward_ev_ebitda,
                    forward_ev_sales,
                )
                section_warnings.extend(builder_warnings)

            elif section_name == "profitability":
                if fetch_errors.get("ratios"):
                    section_warnings.append(
                        f"profitability: failed to fetch ratios data: {fetch_errors['ratios']}"
                    )
                if fetch_errors.get("key_metrics"):
                    section_warnings.append(
                        "profitability: failed to fetch key metrics: "
                        + fetch_errors["key_metrics"]
                    )
                section_data, builder_warnings = _build_profitability(
                    raw_results.get("ratios"),
                    raw_results.get("key_metrics"),
                )
                section_warnings.extend(builder_warnings)

            elif section_name == "balance_sheet":
                if fetch_errors.get("ratios"):
                    section_warnings.append(
                        "balance_sheet: failed to fetch ratios data: "
                        + fetch_errors["ratios"]
                    )
                if fetch_errors.get("key_metrics"):
                    section_warnings.append(
                        "balance_sheet: failed to fetch key metrics: "
                        + fetch_errors["key_metrics"]
                    )
                section_data, builder_warnings = _build_balance_sheet(
                    raw_results.get("ratios"),
                    raw_results.get("key_metrics"),
                )
                section_warnings.extend(builder_warnings)

            elif section_name == "quality":
                if fetch_errors.get("income_statements"):
                    section_warnings.append(
                        "quality: failed to fetch income statements: "
                        + fetch_errors["income_statements"]
                    )
                if fetch_errors.get("cash_flow_statements"):
                    section_warnings.append(
                        "quality: failed to fetch cash flow statements: "
                        + fetch_errors["cash_flow_statements"]
                    )
                if fetch_errors.get("key_metrics"):
                    section_warnings.append(
                        "quality: failed to fetch key metrics: "
                        + fetch_errors["key_metrics"]
                    )
                section_data, builder_warnings = _build_quality(
                    raw_results.get("income_statements"),
                    raw_results.get("cash_flow_statements"),
                    raw_results.get("key_metrics"),
                )
                section_warnings.extend(builder_warnings)

            elif section_name == "technicals":
                section_data, builder_warnings = _build_technicals(raw_results.get("technicals"))
                section_warnings.extend(builder_warnings)

            elif section_name == "chart":
                if fetch_errors.get("chart"):
                    section_warnings.append(
                        f"chart: failed to fetch historical prices: {fetch_errors['chart']}"
                    )
                section_data, builder_warnings = _build_chart(raw_results.get("chart"))
                section_warnings.extend(builder_warnings)

            if section_data is None:
                response["sections_failed"].append(section_name)
            else:
                response[section_name] = section_data
                response["sections_included"].append(section_name)

            response["warnings"].extend(section_warnings)

        return response

    except Exception as exc:
        return {"status": "error", "error": str(exc)}
    finally:
        sys.stdout = _saved
