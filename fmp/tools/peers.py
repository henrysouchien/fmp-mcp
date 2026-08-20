"""
MCP Tool: compare_peers

Exposes peer comparison as an MCP tool for AI invocation.

Usage (from Claude):
    "Compare AAPL to its peers" -> compare_peers(symbol="AAPL")
    "Compare NVDA against MSFT and AMD" -> compare_peers(symbol="NVDA", peers="MSFT,AMD")

Architecture note:
- Standalone tool (no portfolio loading, no user context required)
- Wraps FMP stock-peers plus fundamental metric endpoints
- stdout is redirected to stderr to protect MCP JSON-RPC channel from stray prints
"""

import copy
import logging
import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Optional, Literal

from cachetools import TTLCache

from ..client import FMPClient
from fmp._shared.fmp_helpers import (
    compute_forward_ev_sales,
    compute_forward_pe,
)


logger = logging.getLogger(__name__)

# === Constants ===

# Max peers to fetch ratios for (prevents excessive API calls)
MAX_PEERS = 10
_PEER_METRIC_CACHE_TTL_SECONDS = 900
_peer_metric_snapshot_cache: TTLCache[str, dict[str, object]] = TTLCache(
    maxsize=512,
    ttl=_PEER_METRIC_CACHE_TTL_SECONDS,
)
_peer_metric_snapshot_lock = threading.RLock()

# Default metrics for compare_peers summary
# Keys must match the merged peer comparison payload fields
DEFAULT_PEER_METRICS = [
    # Valuation (most common → most niche)
    "forwardPE",
    "priceToEarningsRatioTTM",
    "_computed_forward_ev_sales",
    "priceToFreeCashFlowRatioTTM",
    "priceToBookRatioTTM",
    "forwardPriceToEarningsGrowthRatioTTM",
    # Financials (top-line → bottom-line → cash → absolute)
    "_ttm_revenue",
    "grossProfitMarginTTM",
    "ebitdaMarginTTM",
    "operatingProfitMarginTTM",
    "netProfitMarginTTM",
    "_computed_fcf_margin",
    "_ttm_ebitda",
    "enterpriseValueTTM",
    # Balance Sheet & Returns (leverage → liquidity → returns → yield)
    "netDebtToEBITDATTM",
    "currentRatioTTM",
    "returnOnInvestedCapitalTTM",
    "freeCashFlowYieldTTM",
    "freeCashFlowPerShareTTM",
    "dividendYieldTTM",
]

# Display labels for metric keys
METRIC_LABELS = {
    "forwardPE": "FY1 P/E",
    "priceToEarningsRatioTTM": "P/E (TTM)",
    "priceToBookRatioTTM": "P/B Ratio",
    "priceToFreeCashFlowRatioTTM": "P/FCF",
    "grossProfitMarginTTM": "Gross Margin",
    "operatingProfitMarginTTM": "Operating Margin",
    "netProfitMarginTTM": "Net Margin",
    "ebitdaMarginTTM": "EBITDA Margin",
    "_computed_fcf_margin": "FCF Margin",
    "netDebtToEBITDATTM": "Net Debt/EBITDA",
    "currentRatioTTM": "Current Ratio",
    "returnOnInvestedCapitalTTM": "ROIC",
    "freeCashFlowYieldTTM": "FCF Yield",
    "dividendYieldTTM": "Dividend Yield",
    "forwardPriceToEarningsGrowthRatioTTM": "PEG (FY1)",
    "_computed_forward_ev_sales": "EV/Sales (FY1)",
    "freeCashFlowPerShareTTM": "FCF/Share",
    "_ttm_revenue": "Revenue",
    "_ttm_ebitda": "EBITDA",
    "enterpriseValueTTM": "Enterprise Value",
}


# === Helpers ===

def clear_peer_metric_snapshot_cache() -> None:
    """Clear cached per-ticker peer metric snapshots."""
    with _peer_metric_snapshot_lock:
        _peer_metric_snapshot_cache.clear()


def _get_cached_peer_metric_snapshot(ticker: str) -> dict[str, object] | None:
    with _peer_metric_snapshot_lock:
        cached = _peer_metric_snapshot_cache.get(ticker)
        if not isinstance(cached, dict):
            return None
        return copy.deepcopy(cached)


def _store_peer_metric_snapshot(ticker: str, snapshot: dict[str, object]) -> None:
    with _peer_metric_snapshot_lock:
        _peer_metric_snapshot_cache[ticker] = copy.deepcopy(snapshot)


def _supports_cached_fetch(fmp: object) -> bool:
    return not type(fmp).__module__.startswith("unittest.mock") and callable(
        getattr(type(fmp), "fetch", None)
    )


def _fetch_ratios_and_estimates(
    fmp: FMPClient,
    ticker: str,
) -> tuple[str, dict | None, dict[str, object], str | None, str | None]:
    """Fetch peer comparison metrics for a single ticker.

    Returns:
        (ticker, merged_dict_or_None, raw_payloads_by_endpoint, retrieved_at, error)
    """
    normalized_ticker = (ticker or "").strip().upper()
    if not normalized_ticker:
        return (normalized_ticker, None, {}, None, "ticker is required")

    cached_snapshot = _get_cached_peer_metric_snapshot(normalized_ticker)
    if cached_snapshot is not None:
        merged = cached_snapshot.get("merged_dict")
        raw_payloads = cached_snapshot.get("raw_payloads_per_endpoint")
        retrieved_at = cached_snapshot.get("retrieved_at")
        if isinstance(merged, dict) and isinstance(raw_payloads, dict):
            return (
                normalized_ticker,
                merged,
                raw_payloads,
                str(retrieved_at) if retrieved_at is not None else None,
                None,
            )
        # Defensive compatibility for cache entries created before the shape change.
        return (normalized_ticker, cached_snapshot, {}, None, None)

    ABSOLUTE_METRICS = {
        "_ttm_revenue",
        "_ttm_ebitda",
        "_ttm_ebit",
        "_ttm_depreciation_and_amortization",
        "_ttm_dividends_paid",
        "enterpriseValueTTM",
        "freeCashFlowPerShareTTM",
        "cashAndCashEquivalents",
        "netDebt",
    }

    def _extract_first_row(data: object) -> dict:
        if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
            return data[0]
        if isinstance(data, dict) and data:
            return data
        return {}

    def _fetch(endpoint: str, **params: object) -> tuple[object | None, str | None]:
        try:
            if _supports_cached_fetch(fmp):
                payload = fmp.fetch(endpoint, **params)
                if payload is not None and hasattr(payload, "to_dict"):
                    return payload.to_dict("records"), None
                return payload, None
            return fmp.fetch_raw(endpoint, **params), None
        except Exception as exc:
            return None, str(exc)

    try:
        retrieved_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        request_specs: dict[str, tuple[str, dict[str, object]]] = {
            "ratios": ("ratios_ttm", {"symbol": normalized_ticker}),
            "metrics": ("key_metrics_ttm", {"symbol": normalized_ticker}),
            "quarterly_income": (
                "income_statement",
                {"symbol": normalized_ticker, "limit": 4, "period": "quarter"},
            ),
            "estimates": (
                "analyst_estimates",
                {"symbol": normalized_ticker, "period": "annual", "limit": 4},
            ),
            "profile": ("profile", {"symbol": normalized_ticker}),
            "latest_income": (
                "income_statement",
                {"symbol": normalized_ticker, "limit": 1, "period": "quarter"},
            ),
            "cash_flow": (
                "cash_flow",
                {"symbol": normalized_ticker, "limit": 4, "period": "quarter"},
            ),
            "latest_balance_sheet": (
                "balance_sheet",
                {"symbol": normalized_ticker, "limit": 1, "period": "quarter"},
            ),
            "enterprise_values": (
                "enterprise_values_ttm",
                {"symbol": normalized_ticker},
            ),
        }

        with ThreadPoolExecutor(max_workers=len(request_specs)) as executor:
            future_map = {
                label: executor.submit(_fetch, endpoint, **params)
                for label, (endpoint, params) in request_specs.items()
            }
            responses = {
                label: future.result()
                for label, future in future_map.items()
            }

        ratios_payload, ratios_error = responses["ratios"]
        ratios_dict = _extract_first_row(ratios_payload)
        if not ratios_dict:
            return (
                normalized_ticker,
                None,
                {},
                retrieved_at,
                ratios_error or f"Empty response for {normalized_ticker}",
            )

        metrics_dict: dict[str, object] = {}
        metrics_payload, _ = responses["metrics"]
        metrics_dict = _extract_first_row(metrics_payload)

        income_dict: dict[str, object] = {}
        income_rows: list[dict] = []
        quarterly_income_payload, _ = responses["quarterly_income"]
        if isinstance(quarterly_income_payload, list):
            income_rows = [row for row in quarterly_income_payload if isinstance(row, dict)]
        elif isinstance(quarterly_income_payload, dict):
            income_rows = [quarterly_income_payload]

        if income_rows:
            rev_values = [row["revenue"] for row in income_rows if row.get("revenue") is not None]
            ebitda_values = [row["ebitda"] for row in income_rows if row.get("ebitda") is not None]
            ebit_values = [row["ebit"] for row in income_rows if row.get("ebit") is not None]
            dna_values = [
                row["depreciationAndAmortization"]
                for row in income_rows
                if row.get("depreciationAndAmortization") is not None
            ]
            if len(rev_values) == 4:
                income_dict["_ttm_revenue"] = sum(rev_values)
            if len(ebitda_values) == 4:
                income_dict["_ttm_ebitda"] = sum(ebitda_values)
            if len(ebit_values) == 4:
                income_dict["_ttm_ebit"] = sum(ebit_values)
            if len(dna_values) == 4:
                income_dict["_ttm_depreciation_and_amortization"] = sum(dna_values)

        estimates: list[dict] | None = None
        estimates_payload, estimates_error = responses["estimates"]
        if estimates_error is not None:
            estimates = None
        elif isinstance(estimates_payload, dict):
            estimates = [estimates_payload]
        elif isinstance(estimates_payload, list):
            estimates = [
                row for row in estimates_payload
                if isinstance(row, dict)
            ]
        else:
            estimates = []

        price = None
        profile_payload, _ = responses["profile"]
        profile_row = _extract_first_row(profile_payload)
        price = profile_row.get("price")

        last_reported_date = None
        latest_income_payload, _ = responses["latest_income"]
        latest_income_row = _extract_first_row(latest_income_payload)
        if latest_income_row.get("date"):
            last_reported_date = str(latest_income_row["date"])[:10]

        forward_pe_result = compute_forward_pe(
            price,
            estimates,
            last_reported_date,
        )

        cash_flow_payload, _ = responses["cash_flow"]
        cash_flow_rows: list[dict] = []
        if isinstance(cash_flow_payload, list):
            cash_flow_rows = [row for row in cash_flow_payload if isinstance(row, dict)]
        elif isinstance(cash_flow_payload, dict):
            cash_flow_rows = [cash_flow_payload]
        fcf_values = [row["freeCashFlow"] for row in cash_flow_rows if row.get("freeCashFlow") is not None]
        free_cash_flow = sum(fcf_values) if len(fcf_values) == 4 else None
        cash_flow_ttm_dict: dict[str, object] = {}
        for field in (
            "freeCashFlow",
            "netDividendsPaid",
            "commonDividendsPaid",
            "depreciationAndAmortization",
            "operatingCashFlow",
            "capitalExpenditure",
        ):
            values = [row[field] for row in cash_flow_rows if row.get(field) is not None]
            if len(values) == 4:
                cash_flow_ttm_dict[field] = sum(values)
        revenue = income_dict.get("_ttm_revenue")
        if free_cash_flow is not None and revenue is not None and revenue > 0:
            income_dict["_computed_fcf_margin"] = free_cash_flow / revenue

        latest_balance_sheet_payload, _ = responses["latest_balance_sheet"]
        balance_sheet_dict = _extract_first_row(latest_balance_sheet_payload)

        enterprise_values_payload, _ = responses["enterprise_values"]
        enterprise_values_dict = _extract_first_row(enterprise_values_payload)
        if not enterprise_values_dict:
            enterprise_values_dict = _build_enterprise_values_payload(
                ratios_dict,
                metrics_dict,
                income_dict,
            )

        merged = {**metrics_dict, **income_dict, **ratios_dict}
        for field in ("cashAndCashEquivalents", "netDebt"):
            if field in balance_sheet_dict:
                merged[field] = balance_sheet_dict[field]
        merged["forwardPE"] = forward_pe_result.get("forward_pe")
        merged["forwardPEBasis"] = forward_pe_result.get("forward_pe_basis")
        merged["forwardPEFiscalPeriod"] = forward_pe_result.get("fiscal_period")
        merged["forwardPEAnalystCount"] = forward_pe_result.get("analyst_count")
        merged["_computed_forward_ev_sales"] = compute_forward_ev_sales(
            merged.get("enterpriseValueTTM"),
            estimates,
            last_reported_date,
        )

        reported_currency = None
        if income_rows:
            reported_currency = income_rows[0].get("reportedCurrency")

        if reported_currency and reported_currency != "USD":
            from fmp.fx import get_spot_fx_rate

            fx_rate = get_spot_fx_rate(reported_currency)
            if fx_rate != 1.0:
                for key in ABSOLUTE_METRICS:
                    if key in merged and merged[key] is not None:
                        try:
                            merged[key] = float(merged[key]) * fx_rate
                        except (TypeError, ValueError):
                            pass
                _convert_payload_values(
                    income_dict,
                    (
                        "_ttm_revenue",
                        "_ttm_ebitda",
                        "_ttm_ebit",
                        "_ttm_depreciation_and_amortization",
                    ),
                    fx_rate,
                )
                _convert_payload_values(
                    cash_flow_ttm_dict,
                    (
                        "freeCashFlow",
                        "netDividendsPaid",
                        "commonDividendsPaid",
                        "depreciationAndAmortization",
                        "operatingCashFlow",
                        "capitalExpenditure",
                    ),
                    fx_rate,
                )
                _convert_payload_values(
                    balance_sheet_dict,
                    ("cashAndCashEquivalents", "netDebt"),
                    fx_rate,
                )

        raw_payloads_per_endpoint: dict[str, object] = {
            "ratios_ttm": ratios_dict,
            "key_metrics_ttm": metrics_dict,
            "income_statement_ttm": {
                "revenue": income_dict.get("_ttm_revenue"),
                "ebitda": income_dict.get("_ttm_ebitda"),
                "ebit": income_dict.get("_ttm_ebit"),
                "depreciationAndAmortization": income_dict.get(
                    "_ttm_depreciation_and_amortization"
                ),
                "reportedCurrency": reported_currency,
            },
            "analyst_estimates": estimates or [],
            "profile": profile_row,
            "cash_flow_ttm": cash_flow_ttm_dict,
            "balance_sheet_ttm": balance_sheet_dict,
            "enterprise_values_ttm": enterprise_values_dict,
            "_metadata": {
                "last_reported_fiscal_date": last_reported_date,
            },
        }

        snapshot = {
            "merged_dict": merged,
            "raw_payloads_per_endpoint": raw_payloads_per_endpoint,
            "retrieved_at": retrieved_at,
        }
        _store_peer_metric_snapshot(normalized_ticker, snapshot)
        return (
            normalized_ticker,
            merged,
            raw_payloads_per_endpoint,
            retrieved_at,
            None,
        )
    except Exception as e:
        return (normalized_ticker, None, {}, None, str(e))


def _build_enterprise_values_payload(
    ratios_dict: dict[str, object],
    metrics_dict: dict[str, object],
    income_dict: dict[str, object],
) -> dict[str, object]:
    enterprise_values: dict[str, object] = {}
    enterprise_value = (
        metrics_dict.get("enterpriseValueTTM")
        or ratios_dict.get("enterpriseValueTTM")
    )
    revenue = income_dict.get("_ttm_revenue")
    ebitda = income_dict.get("_ttm_ebitda")

    enterprise_values["enterpriseValue"] = enterprise_value
    enterprise_values["enterpriseValueTTM"] = enterprise_value
    enterprise_values["evToSales"] = _safe_ratio(enterprise_value, revenue)
    enterprise_values["evToEBITDA"] = (
        metrics_dict.get("evToEBITDA")
        or metrics_dict.get("evToEbitdaTTM")
        or ratios_dict.get("enterpriseValueMultipleTTM")
        or _safe_ratio(enterprise_value, ebitda)
    )
    return enterprise_values


def _safe_ratio(numerator: object, denominator: object) -> float | None:
    try:
        if numerator is None or denominator is None:
            return None
        denominator_float = float(denominator)
        if denominator_float == 0:
            return None
        return float(numerator) / denominator_float
    except (TypeError, ValueError):
        return None


def _convert_payload_values(
    payload: dict[str, object],
    keys: tuple[str, ...],
    fx_rate: float,
) -> None:
    for key in keys:
        if payload.get(key) is None:
            continue
        try:
            payload[key] = float(payload[key]) * fx_rate
        except (TypeError, ValueError):
            pass


def _build_comparison_table(
    ratios_by_ticker: dict[str, dict],
    metrics: list[str],
    tickers_order: list[str],
) -> list[dict]:
    """Pivot ratios into comparison rows (one row per metric, one column per ticker).

    Args:
        ratios_by_ticker: {ticker: {metric_key: value, ...}, ...}
        metrics: List of metric keys to include
        tickers_order: Ordered list of tickers (subject first, then peers)

    Returns:
        List of dicts, one per metric, with ticker columns
    """
    rows = []
    for metric_key in metrics:
        row = {
            "metric": METRIC_LABELS.get(metric_key, metric_key),
            "metric_key": metric_key,
        }
        for ticker in tickers_order:
            ratios = ratios_by_ticker.get(ticker, {})
            row[ticker] = ratios.get(metric_key)
        rows.append(row)
    return rows


def _supports_annual_metric_comparison(metric: str | None) -> bool:
    metric_text = str(metric or "").strip().lower().replace("_", " ")
    return "dividend" in metric_text and "payout" in metric_text


def _build_annual_metric_comparison(
    fmp: FMPClient,
    *,
    tickers_order: list[str],
    metric: str | None,
    fiscal_year: int | str | None,
) -> dict[str, object] | None:
    if not _supports_annual_metric_comparison(metric):
        return None

    requested_fiscal_year = _normalize_fiscal_year(fiscal_year)
    rows: list[dict[str, object]] = []
    missing: list[dict[str, object]] = []

    with ThreadPoolExecutor(max_workers=min(len(tickers_order), 8) or 1) as executor:
        futures = {
            executor.submit(
                _fetch_annual_dividend_payout_row,
                fmp,
                ticker,
                requested_fiscal_year,
            ): ticker
            for ticker in tickers_order
        }
        for future in as_completed(futures):
            ticker = futures[future]
            row, error = future.result()
            if row is not None:
                rows.append(row)
            else:
                missing.append({"ticker": ticker, "error": error or "annual metric unavailable"})

    rows.sort(
        key=lambda row: (
            row.get("dividend_payout_ratio") is None,
            -(float(row.get("dividend_payout_ratio") or 0.0)),
            str(row.get("ticker") or ""),
        )
    )
    for index, row in enumerate(rows, start=1):
        row["rank"] = index

    return {
        "metric_key": "dividend_payout_ratio",
        "metric": "Dividend payout ratio",
        "period": "annual",
        "requested_fiscal_year": requested_fiscal_year,
        "basis": (
            "abs(commonDividendsPaid or dividendsPaid or netDividendsPaid) / netIncome "
            "from annual cash-flow data; net income falls back to annual income statement "
            "when unavailable in cash-flow data."
        ),
        "rows": rows,
        "missing": sorted(missing, key=lambda item: str(item.get("ticker") or "")),
    }


def _fetch_annual_dividend_payout_row(
    fmp: FMPClient,
    ticker: str,
    fiscal_year: str | None,
) -> tuple[dict[str, object] | None, str | None]:
    try:
        cash_flow_payload = fmp.fetch_raw(
            "cash_flow",
            symbol=ticker,
            period="annual",
            limit=10,
        )
        income_payload = fmp.fetch_raw(
            "income_statement",
            symbol=ticker,
            period="annual",
            limit=10,
        )
    except Exception as exc:
        return None, str(exc)

    cash_flow_rows = _record_list(cash_flow_payload)
    income_rows = _record_list(income_payload)
    cash_flow_row = _select_annual_row(cash_flow_rows, fiscal_year)
    if cash_flow_row is None:
        return None, f"cash_flow annual row not found for fiscal_year={fiscal_year or 'latest'}"

    row_fiscal_year = _row_fiscal_year(cash_flow_row)
    income_row = _select_annual_row(income_rows, row_fiscal_year)
    net_income = _number_or_none(cash_flow_row.get("netIncome"))
    if net_income is None and income_row is not None:
        net_income = _number_or_none(income_row.get("netIncome"))

    dividends = _first_number(
        cash_flow_row,
        ("commonDividendsPaid", "dividendsPaid", "netDividendsPaid"),
    )
    if dividends is None:
        return None, "annual dividend cash-flow value not found"
    if net_income in (None, 0):
        return None, "annual net income unavailable or zero"

    dividends_paid = abs(dividends)
    ratio = dividends_paid / net_income
    return (
        {
            "ticker": ticker,
            "fiscal_year": row_fiscal_year,
            "date": cash_flow_row.get("date"),
            "net_income": net_income,
            "common_dividends_paid": dividends_paid,
            "dividend_payout_ratio": ratio,
            "dividend_payout_ratio_percent": ratio * 100,
            "source_endpoint": "cash_flow",
        },
        None,
    )


def _record_list(payload: object) -> list[dict[str, object]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        return [payload]
    return []


def _select_annual_row(
    rows: list[dict[str, object]],
    fiscal_year: str | None,
) -> dict[str, object] | None:
    if not rows:
        return None
    if fiscal_year:
        for row in rows:
            if _row_fiscal_year(row) == fiscal_year:
                return row
        return None
    return rows[0]


def _row_fiscal_year(row: dict[str, object]) -> str | None:
    for key in ("fiscalYear", "calendarYear", "year"):
        value = row.get(key)
        normalized = _normalize_fiscal_year(value)
        if normalized:
            return normalized
    date_value = str(row.get("date") or "").strip()
    if len(date_value) >= 4 and date_value[:4].isdigit():
        return date_value[:4]
    return None


def _normalize_fiscal_year(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    match = re.search(r"(\d{4})", text)
    if match:
        return match.group(1)
    if text.upper().startswith("FY"):
        suffix = text[2:].strip()
        if suffix.isdigit() and len(suffix) == 2:
            return f"20{suffix}"
    return text


def _first_number(row: dict[str, object], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        value = _number_or_none(row.get(key))
        if value is not None:
            return value
    return None


def _number_or_none(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# === Tool ===

def compare_peers(
    symbol: str,
    peers: Optional[str] = None,
    limit: int = 5,
    format: Literal["full", "summary"] = "summary",
    *,
    editorial_peer_set: list[Any] | None = None,
    peer_context: Literal["operating", "valuation", "capital_allocation"] | None = None,
    metric: str | None = None,
    fiscal_year: int | str | None = None,
    include_source_payloads: bool = False,
) -> dict:
    """
    Compare a stock against its peers on key financial ratios.

    Args:
        symbol: Stock symbol to compare (e.g., "AAPL").
        peers: Optional comma-separated peer tickers (e.g., "MSFT,GOOGL,META").
            If not provided, peers are auto-discovered via FMP stock-peers endpoint.
        limit: Maximum number of peers to include (default: 5, max: 10).
        format: "summary" for comparison table with key metrics,
            "full" for all TTM ratios per ticker.
        editorial_peer_set: Optional Track C editorial peer universe.
            Ignored when peers is explicitly provided.
        peer_context: Optional context for metric-aware peer selection.
            Use "capital_allocation" for payout, dividend, buyback, or
            shareholder-return metrics where no-dividend operating peers are
            poor primary comps.
        metric: Optional metric hint used to infer peer_context when omitted.
            For supported fiscal-year metrics such as "dividend_payout_ratio",
            the payload also includes a ranked annual metric comparison.
        fiscal_year: Optional fiscal year for supported annual metric
            comparisons. For "FY24", pass 2024.
        include_source_payloads: Include per-endpoint raw payloads for callers that
            need source provenance.

    Returns:
        dict: Peer comparison data with status field ("success" or "error").
    """
    _saved = sys.stdout
    sys.stdout = sys.stderr
    try:
        symbol = symbol.upper().strip()
        limit = min(limit, MAX_PEERS)

        fmp = FMPClient()

        # Step 1: Determine peer list
        peer_source = "auto"
        peer_resolution: dict[str, object] | None = None
        if peers:
            # Manual peer list
            peer_list = [t.strip().upper() for t in peers.split(",") if t.strip()]
            peer_source = "explicit"
        elif editorial_peer_set is not None or peer_context is not None or metric is not None:
            try:
                from utils.peer_resolver import PeerResolutionError, resolve_metric_peer_universe
            except ImportError:
                # utils.peer_resolver is monorepo-only and is not vendored into the
                # standalone fmp-mcp wheel. Return the manual-peer hint instead of
                # letting the ModuleNotFoundError surface as a NameError on the
                # `except PeerResolutionError` clause below.
                logger.warning(
                    "Metric peer resolver unavailable for %s; provide peers manually",
                    symbol,
                    exc_info=True,
                )
                return {
                    "status": "error",
                    "error": (
                        f"Metric-based peer resolution is unavailable for {symbol} in "
                        "this deployment. Provide peers manually with the 'peers' "
                        "parameter (e.g., peers='MSFT,GOOGL,META')."
                    ),
                }
            try:
                resolution = resolve_metric_peer_universe(
                    symbol,
                    editorial_peer_set,
                    fmp_client=fmp,
                    peer_context=peer_context,
                    metric=metric,
                    limit=limit,
                )
                peer_list = resolution.peers
                peer_source = resolution.source
                peer_resolution = resolution.to_dict()
            except PeerResolutionError:
                return {
                    "status": "error",
                    "error": (
                        f"No peers found for {symbol}. This endpoint works best for "
                        "US large/mid-cap stocks. Try providing peers manually with "
                        "the 'peers' parameter (e.g., peers='MSFT,GOOGL,META')."
                    ),
                }
            except Exception as e:
                return {
                    "status": "error",
                    "error": f"Failed to fetch peers for {symbol}: {e}",
                }
        else:
            # Auto-discover peers
            peer_list = []
            try:
                from core.proxy_builder import (
                    SubindustryPeerGenerationError,
                    get_subindustry_peers_from_ticker,
                )
            except Exception:
                logger.warning(
                    "Custom peer discovery import unavailable for %s; falling back to FMP stock_peers",
                    symbol,
                    exc_info=True,
                )
            else:
                try:
                    peer_list = get_subindustry_peers_from_ticker(symbol)
                    peer_list = list(dict.fromkeys(peer_list))
                except SubindustryPeerGenerationError as e:
                    return {
                        "status": "error",
                        "error": f"Failed to generate custom peers for {symbol}: {e}",
                    }

            if not peer_list:
                try:
                    peers_data = fmp.fetch_raw("stock_peers", symbol=symbol)
                except Exception as e:
                    return {
                        "status": "error",
                        "error": f"Failed to fetch peers for {symbol}: {e}",
                    }

                # Extract peer tickers from response
                # FMP returns either [{peersList: [...]}] (old) or [{symbol, ...}, ...] (new)
                if isinstance(peers_data, list) and len(peers_data) > 0:
                    if isinstance(peers_data[0], dict) and "peersList" in peers_data[0]:
                        peer_list = peers_data[0]["peersList"]
                    elif isinstance(peers_data[0], dict) and "symbol" in peers_data[0]:
                        peer_list = [p["symbol"] for p in peers_data if isinstance(p, dict) and "symbol" in p]
                    else:
                        peer_list = []
                elif isinstance(peers_data, dict):
                    peer_list = peers_data.get("peersList", [])
                else:
                    peer_list = []

            if not peer_list:
                return {
                    "status": "error",
                    "error": (
                        f"No peers found for {symbol}. This endpoint works best for "
                        "US large/mid-cap stocks. Try providing peers manually with "
                        "the 'peers' parameter (e.g., peers='MSFT,GOOGL,META')."
                    ),
                }

        # Remove the subject from peer list if present (it will be added separately)
        peer_list = [t for t in peer_list if t != symbol]

        # Cap to limit
        peer_list = peer_list[:limit]

        # Build full ticker list: subject first, then peers
        all_tickers = [symbol] + peer_list

        # Step 2: Fetch ratios for all tickers in parallel
        ratios_by_ticker: dict[str, dict] = {}
        raw_payloads_by_ticker: dict[str, dict[str, object]] = {}
        retrieved_at_by_ticker: dict[str, str] = {}
        failed_tickers: list[str] = []

        with ThreadPoolExecutor(max_workers=min(len(all_tickers), 6)) as executor:
            futures = {
                executor.submit(_fetch_ratios_and_estimates, fmp, ticker): ticker
                for ticker in all_tickers
            }
            for future in as_completed(futures):
                ticker, ratios, raw_payloads, retrieved_at, error = future.result()
                if ratios is not None:
                    ratios_by_ticker[ticker] = ratios
                    raw_payloads_by_ticker[ticker] = raw_payloads
                    if retrieved_at is not None:
                        retrieved_at_by_ticker[ticker] = retrieved_at
                else:
                    failed_tickers.append(ticker)

        # Check if primary symbol failed
        if symbol not in ratios_by_ticker:
            return {
                "status": "error",
                "error": (
                    f"Failed to fetch ratios for primary symbol {symbol}. "
                    "Cannot build comparison without the subject's data."
                ),
            }

        # Check if all peers failed
        successful_peers = [t for t in peer_list if t in ratios_by_ticker]
        if not successful_peers:
            return {
                "status": "error",
                "error": (
                    f"Failed to fetch ratios for all peers of {symbol}. "
                    "No comparison data available."
                ),
                "failed_tickers": failed_tickers,
            }

        # Build ordered ticker list for output (subject first, then successful peers)
        tickers_order = [symbol] + successful_peers
        annual_metric_comparison = _build_annual_metric_comparison(
            fmp,
            tickers_order=tickers_order,
            metric=metric,
            fiscal_year=fiscal_year,
        )

        # Step 3: Build output
        if format == "full":
            payload = {
                "status": "success",
                "subject": symbol,
                "peers": successful_peers,
                "peer_count": len(successful_peers),
                "peer_source": peer_source,
                "ratios": {t: ratios_by_ticker[t] for t in tickers_order},
                "failed_tickers": failed_tickers,
            }
            if peer_resolution is not None:
                payload["peer_context"] = peer_resolution.get("peer_context")
                payload["peer_resolution"] = peer_resolution
            if annual_metric_comparison is not None:
                payload["annual_metric_comparison"] = annual_metric_comparison
            if include_source_payloads:
                payload["raw_payloads"] = {
                    t: raw_payloads_by_ticker.get(t, {}) for t in tickers_order
                }
                payload["retrieved_at"] = {
                    t: retrieved_at_by_ticker.get(t) for t in tickers_order
                }
            if failed_tickers:
                payload["status"] = "error"
                payload["error"] = (
                    "Failed to fetch ratios for requested peer(s): "
                    f"{', '.join(failed_tickers)}."
                )
            return payload

        # Summary format: comparison table with default or specified metrics
        metrics = DEFAULT_PEER_METRICS
        comparison = _build_comparison_table(ratios_by_ticker, metrics, tickers_order)

        payload = {
            "status": "success",
            "subject": symbol,
            "peers": successful_peers,
            "peer_count": len(successful_peers),
            "peer_source": peer_source,
            "comparison": comparison,
            "forward_pe_fiscal_periods": {
                ticker: ratios_by_ticker[ticker].get("forwardPEFiscalPeriod")
                for ticker in tickers_order
            },
            "failed_tickers": failed_tickers,
        }
        if peer_resolution is not None:
            payload["peer_context"] = peer_resolution.get("peer_context")
            payload["peer_resolution"] = peer_resolution
        if annual_metric_comparison is not None:
            payload["annual_metric_comparison"] = annual_metric_comparison
        if include_source_payloads:
            payload["raw_payloads"] = {
                t: raw_payloads_by_ticker.get(t, {}) for t in tickers_order
            }
            payload["retrieved_at"] = {
                t: retrieved_at_by_ticker.get(t) for t in tickers_order
            }
        if failed_tickers:
            payload["status"] = "error"
            payload["error"] = (
                "Failed to fetch ratios for requested peer(s): "
                f"{', '.join(failed_tickers)}."
            )
        return payload

    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
        }
    finally:
        sys.stdout = _saved
