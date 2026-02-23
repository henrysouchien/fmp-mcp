"""
MCP Tool: get_institutional_ownership

Institutional ownership analytics for a single symbol.
"""

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Literal, Optional

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


def _extract_as_of(records: list[dict]) -> Optional[str]:
    """Extract an as-of timestamp/date when present."""
    for record in records:
        value = _first_non_null(
            record,
            ["timestamp", "lastUpdated", "filingDate", "date", "reportDate", "acceptedDate"],
        )
        if value is not None:
            return str(value)
    return None


def _add_source_status(
    source_status: dict,
    section: str,
    ok: bool,
    count: int,
    as_of: Optional[str] = None,
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
    fallback_endpoint: Optional[str] = None,
    **params,
) -> dict:
    """Fetch records via FMPClient.fetch and normalize to a structured result.

    If ``fallback_endpoint`` is provided and the primary fetch fails with a 402
    (plan-tier restriction), the fallback endpoint is tried automatically.
    """
    try:
        df = client.fetch(endpoint_name, use_cache=use_cache, **params)
        if df is not None and not df.empty:
            return {"ok": True, "data": df.to_dict("records"), "error": None}
        return {"ok": True, "data": [], "error": None}
    except FMPEmptyResponseError:
        return {"ok": True, "data": [], "error": None}
    except Exception as e:
        if fallback_endpoint and "402" in str(e):
            try:
                # v3 endpoints may accept fewer params; filter to symbol only
                fb_params = {k: v for k, v in params.items() if k == "symbol"}
                df = client.fetch(fallback_endpoint, use_cache=use_cache, **fb_params)
                if df is not None and not df.empty:
                    return {"ok": True, "data": df.to_dict("records"), "error": None}
                return {"ok": True, "data": [], "error": None}
            except FMPEmptyResponseError:
                return {"ok": True, "data": [], "error": None}
            except Exception as e2:
                return {"ok": False, "data": [], "error": str(e2)}
        return {"ok": False, "data": [], "error": str(e)}


def _format_holder_summary(records: list[dict], limit: int) -> list[dict]:
    """Summarize top institutional holders."""
    holders = []
    for record in records:
        shares = _safe_float(
            _first_non_null(record, ["sharesNumber", "shares", "sharesHeld", "sharesAmount"])
        )
        item = {
            "holder": _first_non_null(record, ["holder", "investorName", "institutionName", "name"]) or "",
            "shares": int(shares) if shares is not None else None,
            "change_shares": _safe_float(
                _first_non_null(record, ["changeInSharesNumber", "changeInShares", "change"])
            ),
            "weight_pct": _safe_float(
                _first_non_null(record, ["portfolioPercent", "weight", "weightPercentage"])
            ),
            "change_pct": _safe_float(
                _first_non_null(record, ["changePercent", "changeInSharesPercentage", "changePercentage"])
            ),
        }
        holders.append(item)

    holders.sort(
        key=lambda h: (
            h.get("shares") is None,
            -(h.get("shares") if h.get("shares") is not None else 0),
        ),
    )
    return holders[:limit]


def get_institutional_ownership(
    symbol: str,
    year: Optional[int] = None,
    quarter: Optional[int] = None,
    limit: int = 20,
    format: Literal["summary", "full"] = "summary",
    use_cache: bool = True,
) -> dict:
    """
    Get institutional ownership analytics for a stock.

    Args:
        symbol: Stock symbol (e.g., "AAPL").
        year: Optional filing year.
        quarter: Optional filing quarter (1-4).
        limit: Max holder rows to return in summary mode.
        format: "summary" for normalized top holders, "full" for raw records.
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
            "holders": [],
            "positions_summary": [],
        }

        # Each spec: (endpoint, params, optional_v3_fallback)
        fetch_specs = {
            "holders": (
                "institutional_holders",
                {
                    "symbol": normalized_symbol,
                    "year": year,
                    "quarter": quarter,
                    "limit": limit,
                },
                "institutional_holders_v3",  # v3 fallback for 402
            ),
            "positions_summary": (
                "institutional_positions_summary",
                {
                    "symbol": normalized_symbol,
                    "year": year,
                    "quarter": quarter,
                },
                None,  # no v3 fallback
            ),
        }

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(
                    _safe_fetch_records,
                    client,
                    endpoint_name,
                    use_cache=use_cache,
                    fallback_endpoint=fallback,
                    **{k: v for k, v in params.items() if v is not None},
                ): section
                for section, (endpoint_name, params, fallback) in fetch_specs.items()
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
                "error": "Failed to fetch all requested institutional ownership sources.",
                "symbol": normalized_symbol,
                "source_status": source_status,
                "warnings": warnings,
            }

        holders = raw_sections["holders"]
        positions_summary = raw_sections["positions_summary"]

        if format == "full":
            top_holders = holders
            positions_out = positions_summary
        else:
            top_holders = _format_holder_summary(holders, limit)
            positions_out = positions_summary[0] if positions_summary else {}

        return {
            "status": "success",
            "symbol": normalized_symbol,
            "source_status": source_status,
            "warnings": warnings,
            "holder_count": len(holders),
            "top_holders": top_holders,
            "positions_summary": positions_out,
        }

    except Exception as e:
        return {"status": "error", "error": str(e)}
    finally:
        sys.stdout = _saved
