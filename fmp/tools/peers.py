"""
MCP Tool: compare_peers

Exposes peer comparison as an MCP tool for AI invocation.

Usage (from Claude):
    "Compare AAPL to its peers" -> compare_peers(symbol="AAPL")
    "Compare NVDA against MSFT and AMD" -> compare_peers(symbol="NVDA", peers="MSFT,AMD")

Architecture note:
- Standalone tool (no portfolio loading, no user context required)
- Wraps FMP stock-peers and ratios-ttm endpoints
- stdout is redirected to stderr to protect MCP JSON-RPC channel from stray prints
"""

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Literal

from ..client import FMPClient


# === Constants ===

# Max peers to fetch ratios for (prevents excessive API calls)
MAX_PEERS = 10

# Default metrics for compare_peers summary
# Keys must match the actual FMP ratios-ttm response field names
DEFAULT_PEER_METRICS = [
    "priceToEarningsRatioTTM",
    "priceToBookRatioTTM",
    "priceToSalesRatioTTM",
    "grossProfitMarginTTM",
    "operatingProfitMarginTTM",
    "netProfitMarginTTM",
    "debtToEquityRatioTTM",
    "currentRatioTTM",
    "dividendYieldTTM",
    "priceToEarningsGrowthRatioTTM",
    "enterpriseValueMultipleTTM",
    "freeCashFlowPerShareTTM",
]

# Display labels for metric keys
METRIC_LABELS = {
    "priceToEarningsRatioTTM": "P/E Ratio",
    "priceToBookRatioTTM": "P/B Ratio",
    "priceToSalesRatioTTM": "P/S Ratio",
    "grossProfitMarginTTM": "Gross Margin",
    "operatingProfitMarginTTM": "Operating Margin",
    "netProfitMarginTTM": "Net Margin",
    "debtToEquityRatioTTM": "Debt/Equity",
    "currentRatioTTM": "Current Ratio",
    "dividendYieldTTM": "Dividend Yield",
    "priceToEarningsGrowthRatioTTM": "PEG Ratio",
    "enterpriseValueMultipleTTM": "EV/EBITDA",
    "freeCashFlowPerShareTTM": "FCF/Share",
}


# === Helpers ===

def _fetch_ratios(fmp: FMPClient, ticker: str) -> tuple[str, dict | None, str | None]:
    """Fetch TTM ratios for a single ticker.

    Returns:
        (ticker, ratios_dict_or_None, error_message_or_None)
    """
    try:
        data = fmp.fetch_raw("ratios_ttm", symbol=ticker)
        if isinstance(data, list) and len(data) > 0:
            return (ticker, data[0], None)
        elif isinstance(data, dict) and data:
            return (ticker, data, None)
        else:
            return (ticker, None, f"Empty response for {ticker}")
    except Exception as e:
        return (ticker, None, str(e))


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

        with ThreadPoolExecutor(max_workers=min(len(all_tickers), 5)) as executor:
            futures = {
                executor.submit(_fetch_ratios, fmp, ticker): ticker
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
