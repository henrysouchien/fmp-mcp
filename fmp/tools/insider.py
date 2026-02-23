"""
MCP Tool: get_insider_trades

Insider trade flow and statistics for a single symbol.
"""

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Literal

from ..client import FMPClient
from ..exceptions import FMPEmptyResponseError


def _safe_float(value):
    """Parse numeric values with a None fallback."""
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip().replace("%", "").replace(",", "")
        if not value:
            return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_non_null(record: dict, keys: list[str]):
    """Return first non-null value among candidate keys."""
    for key in keys:
        value = record.get(key)
        if value is not None and value != "":
            return value
    return None


def _extract_as_of(records: list[dict]):
    """Extract an as-of timestamp/date when present."""
    for record in records:
        value = _first_non_null(
            record,
            ["timestamp", "lastUpdated", "filingDate", "transactionDate", "date", "acceptedDate"],
        )
        if value is not None:
            return str(value)
    return None


def _add_source_status(
    source_status: dict,
    section: str,
    ok: bool,
    count: int,
    as_of: str | None = None,
) -> None:
    """Set source status entry with standard fields."""
    entry = {"ok": ok, "count": count}
    if as_of:
        entry["as_of"] = as_of
    source_status[section] = entry


def _safe_fetch_records(
    client: FMPClient,
    endpoint_name: str,
    use_cache: bool = True,
    **params,
) -> dict:
    """Fetch records via FMPClient.fetch and normalize to a structured result."""
    try:
        df = client.fetch(endpoint_name, use_cache=use_cache, **params)
        if df is not None and not df.empty:
            return {"ok": True, "data": df.to_dict("records"), "error": None}
        return {"ok": True, "data": [], "error": None}
    except FMPEmptyResponseError:
        return {"ok": True, "data": [], "error": None}
    except Exception as e:
        return {"ok": False, "data": [], "error": str(e)}


def _normalize_trade_type(raw_value) -> str:
    """Map raw transaction labels into buy/sell buckets when possible."""
    text = str(raw_value or "").strip().lower()
    if not text:
        return ""

    buy_tokens = ["buy", "purchase", "acquisition", "acquired", "acquire", "p-"]
    sell_tokens = ["sell", "disposition", "disposed", "dispose", "s-"]

    if any(token in text for token in buy_tokens):
        return "buy"
    if any(token in text for token in sell_tokens):
        return "sell"
    return text


def _format_trade_summary(records: list[dict], limit: int) -> list[dict]:
    """Summarize recent insider trades."""
    trades = []
    for record in records:
        shares = _safe_float(
            _first_non_null(record, ["securitiesTransacted", "shares", "sharesNumber"])
        )
        price = _safe_float(_first_non_null(record, ["price", "pricePerShare"]))
        value = _safe_float(
            _first_non_null(record, ["value", "transactionValue", "securitiesTransactedValue"])
        )
        if value is None and shares is not None and price is not None:
            value = round(shares * price, 2)

        trades.append({
            "date": str(
                _first_non_null(record, ["transactionDate", "filingDate", "date", "acceptedDate"]) or ""
            )[:10],
            "insider": _first_non_null(record, ["reportingName", "insiderName", "name"]) or "",
            "title": _first_non_null(
                record,
                ["typeOfOwner", "officerTitle", "title", "reportingTitle"],
            )
            or "",
            "type": _normalize_trade_type(
                _first_non_null(record, ["transactionType", "acquisitionOrDisposition", "type"])
            ),
            "shares": int(shares) if shares is not None else None,
            "price": price,
            "value": value,
        })

    trades.sort(key=lambda t: t.get("date") or "", reverse=True)
    return trades[:limit]


def get_insider_trades(
    symbol: str,
    limit: int = 20,
    format: Literal["summary", "full"] = "summary",
    use_cache: bool = True,
) -> dict:
    """
    Get insider trade activity and statistics for a stock.

    Args:
        symbol: Stock symbol (required, e.g., "AAPL").
        limit: Max trades returned in summary mode.
        format: "summary" for normalized trades, "full" for raw records.
        use_cache: Use cached FMP data when available.

    Returns:
        dict with status field ("success" or "error").
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

        limit = max(1, min(100, int(limit)))

        client = FMPClient()
        source_status: dict[str, dict] = {}
        warnings: list[str] = []
        raw_sections: dict[str, list[dict]] = {
            "trades": [],
            "statistics": [],
        }

        fetch_specs = {
            "trades": (
                "insider_trades_search",
                {
                    "symbol": normalized_symbol,
                    "limit": limit,
                },
            ),
            "statistics": (
                "insider_trade_statistics",
                {
                    "symbol": normalized_symbol,
                },
            ),
        }

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(
                    _safe_fetch_records,
                    client,
                    endpoint_name,
                    use_cache=use_cache,
                    **params,
                ): section
                for section, (endpoint_name, params) in fetch_specs.items()
            }

            for future in as_completed(futures):
                section = futures[future]
                result = future.result()
                if result["ok"]:
                    records = result["data"]
                    raw_sections[section] = records
                    _add_source_status(
                        source_status,
                        section,
                        True,
                        len(records),
                        _extract_as_of(records),
                    )
                else:
                    raw_sections[section] = []
                    _add_source_status(source_status, section, False, 0)
                    warnings.append(f"{section}: {result['error']}")

        if all(not source_status[section]["ok"] for section in fetch_specs):
            return {
                "status": "error",
                "error": "Failed to fetch all requested insider trading sources.",
                "symbol": normalized_symbol,
                "source_status": source_status,
                "warnings": warnings,
            }

        trades = raw_sections["trades"]
        statistics = raw_sections["statistics"]

        if format == "full":
            recent_trades = trades
            stats_out = statistics
        else:
            recent_trades = _format_trade_summary(trades, limit)
            stats_out = statistics[0] if statistics else {}

        return {
            "status": "success",
            "symbol": normalized_symbol,
            "source_status": source_status,
            "warnings": warnings,
            "trade_count": len(trades),
            "statistics": stats_out,
            "recent_trades": recent_trades,
        }

    except Exception as e:
        return {"status": "error", "error": str(e)}
    finally:
        sys.stdout = _saved
