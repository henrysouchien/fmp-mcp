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
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Literal

from cachetools import TTLCache

from ..client import FMPClient
from utils.fmp_helpers import (
    compute_forward_ev_ebitda,
    compute_forward_ev_sales,
    compute_forward_pe,
)


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
    "_computed_forward_ev_ebitda",
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
    "forwardPE": "Fwd P/E",
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
    "_computed_forward_ev_ebitda": "EV/EBITDA (FY1)",
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


def _fetch_ratios_and_estimates(fmp: FMPClient, ticker: str) -> tuple[str, dict | None, str | None]:
    """Fetch peer comparison metrics for a single ticker.

    Returns:
        (ticker, ratios_dict_or_None, error_message_or_None)
    """
    normalized_ticker = (ticker or "").strip().upper()
    if not normalized_ticker:
        return (normalized_ticker, None, "ticker is required")

    cached_snapshot = _get_cached_peer_metric_snapshot(normalized_ticker)
    if cached_snapshot is not None:
        return (normalized_ticker, cached_snapshot, None)
    ABSOLUTE_METRICS = {
        "_ttm_revenue",
        "_ttm_ebitda",
        "enterpriseValueTTM",
        "freeCashFlowPerShareTTM",
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
            return (normalized_ticker, None, ratios_error or f"Empty response for {normalized_ticker}")

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
            if len(rev_values) == 4:
                income_dict["_ttm_revenue"] = sum(rev_values)
            if len(ebitda_values) == 4:
                income_dict["_ttm_ebitda"] = sum(ebitda_values)

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
        revenue = income_dict.get("_ttm_revenue")
        if free_cash_flow is not None and revenue is not None and revenue > 0:
            income_dict["_computed_fcf_margin"] = free_cash_flow / revenue

        merged = {**metrics_dict, **income_dict, **ratios_dict}
        merged["forwardPE"] = forward_pe_result.get("forward_pe")
        merged["_computed_forward_ev_ebitda"] = compute_forward_ev_ebitda(
            merged.get("enterpriseValueTTM"),
            estimates,
            last_reported_date,
        )
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

        _store_peer_metric_snapshot(normalized_ticker, merged)
        return (normalized_ticker, merged, None)
    except Exception as e:
        return (normalized_ticker, None, str(e))


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


# === Tool ===

def compare_peers(
    symbol: str,
    peers: Optional[str] = None,
    limit: int = 5,
    format: Literal["full", "summary"] = "summary",
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
        if peers:
            # Manual peer list
            peer_list = [t.strip().upper() for t in peers.split(",") if t.strip()]
        else:
            # Auto-discover peers
            peer_list = []
            try:
                from core.proxy_builder import get_subindustry_peers_from_ticker

                peer_list = get_subindustry_peers_from_ticker(symbol)
                peer_list = list(dict.fromkeys(peer_list))
            except Exception:
                peer_list = []

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
        failed_tickers: list[str] = []

        with ThreadPoolExecutor(max_workers=min(len(all_tickers), 6)) as executor:
            futures = {
                executor.submit(_fetch_ratios_and_estimates, fmp, ticker): ticker
                for ticker in all_tickers
            }
            for future in as_completed(futures):
                ticker, ratios, error = future.result()
                if ratios is not None:
                    ratios_by_ticker[ticker] = ratios
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

        # Step 3: Build output
        if format == "full":
            return {
                "status": "success",
                "subject": symbol,
                "peers": successful_peers,
                "peer_count": len(successful_peers),
                "ratios": {t: ratios_by_ticker[t] for t in tickers_order},
                "failed_tickers": failed_tickers,
            }

        # Summary format: comparison table with default or specified metrics
        metrics = DEFAULT_PEER_METRICS
        comparison = _build_comparison_table(ratios_by_ticker, metrics, tickers_order)

        return {
            "status": "success",
            "subject": symbol,
            "peers": successful_peers,
            "peer_count": len(successful_peers),
            "comparison": comparison,
            "failed_tickers": failed_tickers,
        }

    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
        }
    finally:
        sys.stdout = _saved
