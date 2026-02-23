"""
MCP Tool: get_etf_holdings

ETF/fund composition snapshot across holdings, sectors, countries, and metadata.
"""

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import re
from typing import Literal, Optional

from ..client import FMPClient
from ..exceptions import FMPEmptyResponseError
from ._file_output import FILE_OUTPUT_DIR, write_csv


ETF_INCLUDE_OPTIONS = ["holdings", "sectors", "countries", "info", "exposure", "disclosure"]

_SECTION_TO_ENDPOINT = {
    "holdings": "etf_holdings",
    "sectors": "etf_sector_weightings",
    "countries": "etf_country_weightings",
    "info": "etf_info",
    "exposure": "etf_asset_exposure",
    "disclosure": "etf_disclosure",
}

# v3 legacy fallback endpoints for sections that return 402 on stable API
_SECTION_V3_FALLBACK = {
    "holdings": "etf_holdings_v3",
}


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
            ["timestamp", "lastUpdated", "date", "reportDate", "acceptedDate", "filingDate"],
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


def _summarize_holdings(records: list[dict], limit: int) -> list[dict]:
    """Summarize top ETF holdings."""
    holdings = []
    for record in records:
        holdings.append({
            "asset": _first_non_null(record, ["asset", "holding", "name", "securityName"]) or "",
            "symbol": _first_non_null(record, ["symbol", "assetSymbol", "ticker"]) or "",
            "weight_pct": _safe_float(
                _first_non_null(record, ["weightPercentage", "weight", "allocation"])
            ),
            "shares": _safe_float(_first_non_null(record, ["sharesNumber", "shares"])),
        })

    holdings.sort(
        key=lambda h: (
            h.get("weight_pct") is None,
            -(h.get("weight_pct") if h.get("weight_pct") is not None else 0),
        ),
    )
    return holdings[:limit]


def _summarize_weightings(records: list[dict], key_names: list[str]) -> list[dict]:
    """Summarize sector/country style weighting rows."""
    rows = []
    for record in records:
        name = _first_non_null(record, key_names) or ""
        rows.append({
            "name": name,
            "weight_pct": _safe_float(
                _first_non_null(record, ["weightPercentage", "weight", "allocation"])
            ),
        })

    rows.sort(
        key=lambda r: (
            r.get("weight_pct") is None,
            -(r.get("weight_pct") if r.get("weight_pct") is not None else 0),
        ),
    )
    return rows


def _summarize_info(record: dict) -> dict:
    """Extract key ETF metadata fields from info record."""
    if not record:
        return {}

    return {
        "symbol": _first_non_null(record, ["symbol", "ticker"]),
        "name": _first_non_null(record, ["name", "fundName", "etfName"]),
        "provider": _first_non_null(record, ["issuer", "fundFamily", "provider"]),
        "expense_ratio": _safe_float(
            _first_non_null(record, ["expenseRatio", "expenseRatioPct", "expense_ratio"])
        ),
        "aum": _safe_float(
            _first_non_null(record, ["aum", "assetsUnderManagement", "netAssets", "totalAssets"])
        ),
        "inception_date": _first_non_null(record, ["inceptionDate", "fundInceptionDate", "launchDate"]),
        "asset_class": _first_non_null(record, ["assetClass", "category"]),
    }


def _summarize_disclosure(records: list[dict], limit: int) -> list[dict]:
    """Summarize top disclosure holders."""
    rows = []
    for record in records:
        rows.append({
            "holder": _first_non_null(record, ["holder", "investorName", "name"]) or "",
            "weight_pct": _safe_float(
                _first_non_null(record, ["weightPercentage", "weight", "portfolioPercent"])
            ),
            "shares": _safe_float(_first_non_null(record, ["sharesNumber", "shares"])),
        })

    rows.sort(
        key=lambda r: (
            r.get("weight_pct") is None,
            -(r.get("weight_pct") if r.get("weight_pct") is not None else 0),
        ),
    )
    return rows[:limit]


def get_etf_holdings(
    symbol: str,
    include: Optional[list[str]] = None,
    limit: int = 25,
    format: Literal["summary", "full"] = "summary",
    output: Literal["inline", "file"] = "inline",
    use_cache: bool = True,
) -> dict:
    """
    Get ETF holdings/allocation context across selectable sections.

    Args:
        symbol: ETF symbol (e.g., "SPY", "QQQ").
        include: Optional section subset. Valid: holdings, sectors, countries,
            info, exposure, disclosure. Defaults to all.
        limit: Max items in holdings/exposure/disclosure summary lists.
        format: "summary" for normalized section output, "full" for raw data.
        output: "inline" (default) or "file". Only affects full holdings output.
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
                "error": "symbol is required. Specify an ETF symbol (e.g., 'SPY').",
            }

        limit = max(1, min(200, int(limit)))

        invalid_sections = []
        if include is None:
            requested_sections = ETF_INCLUDE_OPTIONS.copy()
        else:
            requested_sections = []
            for section in include:
                if not isinstance(section, str):
                    invalid_sections.append(str(section))
                    continue
                section_name = section.strip().lower()
                if section_name in ETF_INCLUDE_OPTIONS:
                    if section_name not in requested_sections:
                        requested_sections.append(section_name)
                else:
                    invalid_sections.append(section)

            if not requested_sections:
                return {
                    "status": "error",
                    "error": (
                        "All requested sections are invalid. "
                        f"Valid sections: {', '.join(ETF_INCLUDE_OPTIONS)}"
                    ),
                    "invalid_sections": invalid_sections,
                    "requested_sections": [],
                }

        client = FMPClient()
        source_status: dict[str, dict] = {}
        warnings: list[str] = []
        raw_sections: dict[str, list[dict] | dict] = {
            section: {} if section == "info" else []
            for section in requested_sections
        }

        with ThreadPoolExecutor(max_workers=min(len(requested_sections), 6)) as executor:
            futures = {
                executor.submit(
                    _safe_fetch_records,
                    client,
                    _SECTION_TO_ENDPOINT[section],
                    use_cache=use_cache,
                    fallback_endpoint=_SECTION_V3_FALLBACK.get(section),
                    symbol=normalized_symbol,
                ): section
                for section in requested_sections
            }

            for future in as_completed(futures):
                section = futures[future]
                result = future.result()
                if result["ok"]:
                    records = result["data"]
                    if section == "info":
                        info_record = records[0] if records else {}
                        raw_sections[section] = info_record
                        count = 1 if info_record else 0
                    else:
                        raw_sections[section] = records
                        count = len(records)

                    _add_source_status(
                        source_status,
                        section,
                        True,
                        count,
                        _extract_as_of(records),
                    )
                else:
                    raw_sections[section] = {} if section == "info" else []
                    _add_source_status(source_status, section, False, 0)
                    warnings.append(f"{section}: {result['error']}")

        if requested_sections and all(not source_status[s]["ok"] for s in requested_sections):
            return {
                "status": "error",
                "error": "Failed to fetch all requested ETF sections.",
                "symbol": normalized_symbol,
                "requested_sections": requested_sections,
                "source_status": source_status,
                "warnings": warnings,
                **({"invalid_sections": invalid_sections} if invalid_sections else {}),
            }

        output_mode = "inline"
        if format == "full":
            sections_out = raw_sections
            if output == "file" and "holdings" in requested_sections:
                holdings_records = raw_sections.get("holdings", [])
                if isinstance(holdings_records, list):
                    symbol_slug = re.sub(r"[^A-Za-z0-9._-]+", "_", normalized_symbol).strip("_") or "ETF"
                    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S%f")
                    file_path = FILE_OUTPUT_DIR / f"etf_holdings_{symbol_slug}_{timestamp}.csv"
                    write_csv(holdings_records, file_path)

                    sections_out = dict(raw_sections)
                    sections_out["holdings"] = _build_holdings_file_summary(
                        holdings_records,
                        file_path=str(file_path),
                    )
                    output_mode = "file"
        else:
            sections_out: dict[str, list[dict] | dict] = {}
            for section in requested_sections:
                if section == "holdings":
                    sections_out["holdings"] = _summarize_holdings(
                        raw_sections.get("holdings", []),
                        limit,
                    )
                elif section == "sectors":
                    sections_out["sectors"] = _summarize_weightings(
                        raw_sections.get("sectors", []),
                        ["sector", "name"],
                    )
                elif section == "countries":
                    sections_out["countries"] = _summarize_weightings(
                        raw_sections.get("countries", []),
                        ["country", "name"],
                    )
                elif section == "info":
                    sections_out["info"] = _summarize_info(raw_sections.get("info", {}))
                elif section == "exposure":
                    sections_out["exposure"] = _summarize_holdings(
                        raw_sections.get("exposure", []),
                        limit,
                    )
                elif section == "disclosure":
                    sections_out["disclosure"] = _summarize_disclosure(
                        raw_sections.get("disclosure", []),
                        limit,
                    )

        response = {
            "status": "success",
            "symbol": normalized_symbol,
            "requested_sections": requested_sections,
            "sections": sections_out,
            "source_status": source_status,
            "warnings": warnings,
        }
        if output_mode == "file":
            response["output"] = "file"
        if invalid_sections:
            response["invalid_sections"] = invalid_sections
        return response

    except Exception as e:
        return {"status": "error", "error": str(e)}
    finally:
        sys.stdout = _saved


def _build_holdings_file_summary(records: list[dict], file_path: str) -> dict:
    """Build compact holdings metadata when full holdings are written to file."""
    top_5 = _summarize_holdings(records, 5)
    top_25 = _summarize_holdings(records, 25)
    coverage = sum(
        row["weight_pct"]
        for row in top_25
        if row.get("weight_pct") is not None
    )

    return {
        "holdings_count": len(records),
        "top_5": top_5,
        "weight_coverage_top_25": f"{coverage:.2f}%",
        "file_path": file_path,
        "hint": "Use Read tool with file_path, or Grep to search holdings.",
    }
