"""
MCP Tools: get_economic_data, get_sector_overview, get_market_context

Exposes macroeconomic indicators, economic calendar events, and sector
performance/valuation data as MCP tools.

Usage (from Claude):
    "What's the latest CPI reading?"
    "Is inflation trending up or down?"
    "What economic events are coming up?"
    "What's the fed funds rate?"
    "How are sectors performing today?"
    "Technology sector overview"
    "Which sectors are cheapest?"
    "What's happening in the market?"

Architecture note:
- Standalone market data tools (no portfolio loading required)
- Fetches from FMP economic-indicators, economic-calendar, and sector snapshot endpoints
- stdout is redirected to stderr to protect MCP JSON-RPC channel from stray prints
"""

import math
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Optional, Literal

import pandas as pd

from ..client import FMPClient
from ..exceptions import FMPEmptyResponseError
from ._file_output import FILE_OUTPUT_DIR, write_csv
from ._helpers import _last_trading_day
from fmp._shared.fmp_helpers import compute_forward_pe, first_dataframe_record, parse_fmp_float


# Valid indicator names accepted by the FMP economic-indicators endpoint
VALID_INDICATORS = [
    "GDP",
    "realGDP",
    "CPI",
    "inflationRate",
    "federalFunds",
    "unemploymentRate",
    "totalNonfarmPayroll",
    "initialClaims",
    "consumerSentiment",
    "retailSales",
    "durableGoods",
    "industrialProductionTotalIndex",
    "housingStarts",
    "totalVehicleSales",
    "smoothedUSRecessionProbabilities",
    "30YearFixedRateMortgageAverage",
    "tradeBalanceGoodsAndServices",
]


def _default_date_range(
    from_date: Optional[str],
    to_date: Optional[str],
    default_lookback_days: int = 730,
    default_forward_days: int = 0,
) -> tuple[str, str]:
    """Apply default date range if not specified."""
    today = date.today()
    if from_date is None:
        from_date = (today - timedelta(days=default_lookback_days)).isoformat()
    if to_date is None:
        to_date = (today + timedelta(days=default_forward_days)).isoformat()
    return from_date, to_date


def _compute_trend(values: list[float], window: int = 3) -> str:
    """Determine trend direction from recent values.

    Args:
        values: Time-ordered list of values (oldest first).
        window: Number of recent data points to examine.

    Returns:
        One of: "rising", "falling", "stable", "insufficient_data".
    """
    if len(values) < 2:
        return "insufficient_data"
    recent = values[-window:] if len(values) >= window else values
    if all(recent[i] <= recent[i + 1] for i in range(len(recent) - 1)):
        return "rising"
    elif all(recent[i] >= recent[i + 1] for i in range(len(recent) - 1)):
        return "falling"
    else:
        # Net direction over window
        pct_change = (
            (recent[-1] - recent[0]) / abs(recent[0]) * 100
            if recent[0] != 0
            else 0
        )
        if abs(pct_change) < 0.5:
            return "stable"
        return "rising" if pct_change > 0 else "falling"


def _format_indicator_summary(data: list[dict]) -> dict:
    """Build summary output for indicator mode.

    Args:
        data: List of records from FMP, each with 'date' and 'value' keys.
              Expected in descending date order from the API.

    Returns:
        Summary dict with latest value, trend, period, etc.
    """
    if not data:
        return {
            "data_points": 0,
            "note": "No data returned for this indicator and date range.",
        }

    # FMP returns data in descending order (newest first). Sort ascending for trend.
    sorted_data = sorted(data, key=lambda r: r.get("date", ""))

    values = [float(r["value"]) for r in sorted_data if r.get("value") is not None]
    dates = [r["date"] for r in sorted_data if r.get("date")]

    if not values:
        return {
            "data_points": 0,
            "note": "No numeric values found in indicator data.",
        }

    latest_value = values[-1]
    previous_value = values[-2] if len(values) >= 2 else None
    change = round(latest_value - previous_value, 4) if previous_value is not None else None
    change_pct = (
        round((latest_value - previous_value) / abs(previous_value) * 100, 4)
        if previous_value is not None and previous_value != 0
        else None
    )

    return {
        "latest_value": latest_value,
        "latest_date": dates[-1] if dates else None,
        "previous_value": previous_value,
        "change": change,
        "change_pct": change_pct,
        "trend": _compute_trend(values),
        "data_points": len(values),
        "period": {
            "from": dates[0] if dates else None,
            "to": dates[-1] if dates else None,
        },
    }


def _format_calendar_summary(data: list[dict]) -> dict:
    """Build summary output for calendar mode.

    Args:
        data: List of event records from FMP economic calendar.

    Returns:
        Summary dict with upcoming high-impact events and recent surprises.
    """
    if not data:
        return {
            "event_count": 0,
            "upcoming_high_impact": [],
            "recent_surprises": [],
            "note": "No economic events found for this date range.",
        }

    today_str = date.today().isoformat()
    seven_days_ago = (date.today() - timedelta(days=7)).isoformat()

    # Upcoming high-impact events (top 5)
    upcoming_high_impact = []
    for event in data:
        event_date = event.get("date", "")
        impact = event.get("impact", "")
        if event_date >= today_str and impact and impact.lower() == "high":
            upcoming_high_impact.append({
                "event": event.get("event", ""),
                "date": event_date,
                "country": event.get("country", ""),
                "previous": event.get("previous"),
                "estimate": event.get("estimate"),
                "actual": event.get("actual"),
                "impact": impact,
            })
    # Sort by date, take first 5
    upcoming_high_impact.sort(key=lambda e: e["date"])
    upcoming_high_impact = upcoming_high_impact[:5]

    # Recent surprises: events where actual != estimate (last 7 days)
    recent_surprises = []
    for event in data:
        event_date = event.get("date", "")
        actual = event.get("actual")
        estimate = event.get("estimate")
        if (
            seven_days_ago <= event_date <= today_str
            and actual is not None
            and estimate is not None
            and estimate != 0
        ):
            surprise_pct = round((actual - estimate) / abs(estimate) * 100, 2)
            if abs(surprise_pct) > 0.01:  # Exclude trivially zero differences
                recent_surprises.append({
                    "event": event.get("event", ""),
                    "date": event_date,
                    "estimate": estimate,
                    "actual": actual,
                    "surprise_pct": surprise_pct,
                })
    # Sort by absolute surprise magnitude descending
    recent_surprises.sort(key=lambda e: abs(e["surprise_pct"]), reverse=True)
    recent_surprises = recent_surprises[:5]

    return {
        "event_count": len(data),
        "upcoming_high_impact": upcoming_high_impact,
        "recent_surprises": recent_surprises,
    }


def get_economic_data(
    mode: Literal["indicator", "calendar"] = "indicator",
    indicator_name: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    country: Optional[str] = "US",
    format: Literal["full", "summary"] = "summary",
    limit: Optional[int] = None,
    output: Literal["inline", "file"] = "inline",
    use_cache: bool = True,
) -> dict:
    """
    Get economic indicators or upcoming economic events.

    Fetches macroeconomic data from FRED via FMP. Use indicator mode for
    time series data (GDP, CPI, unemployment, etc.) or calendar mode for
    upcoming/recent economic releases.

    Args:
        mode: Data type to fetch:
            - "indicator": Economic indicator time series (requires indicator_name)
            - "calendar": Upcoming economic events with forecasts and actuals
        indicator_name: Indicator to fetch (required for indicator mode).
            Available: GDP, realGDP, CPI, inflationRate, federalFunds,
            unemploymentRate, totalNonfarmPayroll, initialClaims,
            consumerSentiment, retailSales, durableGoods,
            industrialProductionTotalIndex, housingStarts, totalVehicleSales,
            smoothedUSRecessionProbabilities, 30YearFixedRateMortgageAverage,
            tradeBalanceGoodsAndServices.
        from_date: Start date in YYYY-MM-DD format (optional).
        to_date: End date in YYYY-MM-DD format (optional).
        country: Country filter for calendar mode (ISO code like "US").
            Defaults to "US". Set to None to include all countries.
        format: Output format:
            - "summary": Latest value, trend, and key context
            - "full": Complete time series or event list
        limit: Optional cap for indicator mode (most recent N rows).
        output: "inline" (default) or "file". Applies to indicator full mode.
        use_cache: Use cached data when available (default: True).

    Returns:
        Economic data with status field ("success" or "error").
    """
    _saved = sys.stdout
    sys.stdout = sys.stderr
    try:
        if mode == "indicator":
            return _fetch_indicator(
                indicator_name=indicator_name,
                from_date=from_date,
                to_date=to_date,
                format=format,
                use_cache=use_cache,
                limit=limit,
                output=output,
            )
        elif mode == "calendar":
            return _fetch_calendar(from_date, to_date, country, format, use_cache)
        else:
            return {
                "status": "error",
                "error": f"Invalid mode '{mode}'. Must be 'indicator' or 'calendar'.",
            }

    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
        }
    finally:
        sys.stdout = _saved


def _fetch_indicator(
    indicator_name: Optional[str],
    from_date: Optional[str],
    to_date: Optional[str],
    format: str,
    use_cache: bool,
    limit: Optional[int],
    output: Literal["inline", "file"],
) -> dict:
    """Fetch an economic indicator time series."""

    # Validate indicator_name is provided
    if not indicator_name:
        return {
            "status": "error",
            "error": (
                "indicator_name is required when mode='indicator'. "
                f"Available indicators: {', '.join(VALID_INDICATORS)}"
            ),
        }

    # Validate indicator_name is in the valid list
    if indicator_name not in VALID_INDICATORS:
        return {
            "status": "error",
            "error": (
                f"Invalid indicator '{indicator_name}'. "
                f"Available indicators: {', '.join(VALID_INDICATORS)}"
            ),
        }

    # Apply date defaults: 2 years back to today
    from_date, to_date = _default_date_range(
        from_date, to_date, default_lookback_days=730, default_forward_days=0
    )

    if limit is not None:
        try:
            limit = int(limit)
        except (TypeError, ValueError):
            return {
                "status": "error",
                "error": "limit must be a positive integer.",
            }
        if limit <= 0:
            return {
                "status": "error",
                "error": "limit must be a positive integer.",
            }

    client = FMPClient()
    df = client.fetch(
        "economic_indicators",
        use_cache=use_cache,
        name=indicator_name,
        from_date=from_date,
        to_date=to_date,
    )

    records = df.to_dict("records") if not df.empty else []
    columns = list(df.columns) if not df.empty else []

    if limit is not None:
        records = _slice_most_recent(records, limit)

    if format == "full":
        response = {
            "status": "success",
            "mode": "indicator",
            "indicator": indicator_name,
            "row_count": len(records),
            "columns": columns,
        }
        if output == "file":
            indicator_slug = re.sub(r"[^A-Za-z0-9._-]+", "_", str(indicator_name)).strip("_")
            indicator_slug = indicator_slug or "indicator"
            timestamp = datetime.now().strftime("%Y%m%dT%H%M%S%f")
            file_path = FILE_OUTPUT_DIR / f"economic_{indicator_slug}_{timestamp}.csv"
            write_csv(records, file_path)
            response.update(_format_indicator_summary(records))
            response["output"] = "file"
            response["file_path"] = str(file_path)
            response["hint"] = "Use Read tool with file_path, or Grep to search values."
            return response

        response["output"] = "inline"
        response["data"] = records
        return response

    # Summary format
    summary = _format_indicator_summary(records)
    summary["status"] = "success"
    summary["mode"] = "indicator"
    summary["indicator"] = indicator_name
    return summary


def _slice_most_recent(records: list[dict], limit: int) -> list[dict]:
    """Return the most recent N records while preserving existing sort order."""
    if len(records) <= limit:
        return records

    first_date = records[0].get("date")
    last_date = records[-1].get("date")
    if isinstance(first_date, str) and isinstance(last_date, str):
        # Descending (newest-first): keep head, ascending: keep tail.
        return records[:limit] if first_date >= last_date else records[-limit:]

    return records[:limit]


def _fetch_calendar(
    from_date: Optional[str],
    to_date: Optional[str],
    country: Optional[str],
    format: str,
    use_cache: bool,
) -> dict:
    """Fetch economic calendar events."""

    # Apply date defaults: today to +30 days
    from_date, to_date = _default_date_range(
        from_date, to_date, default_lookback_days=0, default_forward_days=30
    )

    # Validate date range <= 90 days (FMP limit)
    try:
        from_dt = date.fromisoformat(from_date)
        to_dt = date.fromisoformat(to_date)
        if (to_dt - from_dt).days > 90:
            return {
                "status": "error",
                "error": (
                    "Calendar date range cannot exceed 90 days (FMP API limit). "
                    f"Requested range: {from_date} to {to_date} "
                    f"({(to_dt - from_dt).days} days)."
                ),
            }
    except ValueError as e:
        return {
            "status": "error",
            "error": f"Invalid date format: {e}",
        }

    client = FMPClient()
    df = client.fetch(
        "economic_calendar",
        use_cache=use_cache,
        from_date=from_date,
        to_date=to_date,
    )

    records = df.to_dict("records") if not df.empty else []
    # Scrub NaN/NaT values from pandas serialization.
    for rec in records:
        for key, val in rec.items():
            if val is not None and pd.isna(val):
                rec[key] = None
    if country:
        country_upper = str(country).upper()
        records = [
            r for r in records
            if str(r.get("country") or "").upper() == country_upper
        ]

    if format == "full":
        return {
            "status": "success",
            "mode": "calendar",
            "data": records,
            "row_count": len(records),
            "columns": list(df.columns) if not df.empty else [],
            "period": {"from": from_date, "to": to_date},
            "country": country,
        }

    # Summary format
    summary = _format_calendar_summary(records)
    summary["status"] = "success"
    summary["mode"] = "calendar"
    summary["period"] = {"from": from_date, "to": to_date}
    summary["country"] = country
    return summary


# ---------------------------------------------------------------------------
# Shared helper: safe fetch
# ---------------------------------------------------------------------------


def _safe_fetch(
    client: FMPClient,
    endpoint_name: str,
    use_cache: bool = True,
    **params,
) -> pd.DataFrame:
    """Fetch from FMP, returning empty DataFrame on error."""
    try:
        return client.fetch(endpoint_name, use_cache=use_cache, **params)
    except Exception:
        return pd.DataFrame()


MAX_COMPARISON_SYMBOLS = 10
PE_PREMIUM_THRESHOLD_PCT = 5.0


# ---------------------------------------------------------------------------
# get_sector_overview
# ---------------------------------------------------------------------------


def get_sector_overview(
    date: Optional[str] = None,
    sector: Optional[str] = None,
    symbols: Optional[list[str]] = None,
    level: Literal["sector", "industry"] = "sector",
    format: Literal["full", "summary"] = "summary",
    use_cache: bool = True,
) -> dict:
    """
    Get sector or industry performance and valuation overview.

    Combines daily performance snapshot and P/E ratio snapshot into a
    unified view. Supports both sector-level and industry-level granularity.

    Args:
        date: Snapshot date in YYYY-MM-DD format (optional, defaults to latest).
        sector: Filter to one sector/industry name (e.g., "Technology", "Energy").
            If not provided, returns all sectors/industries.
        symbols: Optional list of stock symbols for per-symbol P/E comparison mode.
            When provided, compares each stock's TTM P/E against the selected level's
            sector/industry benchmark P/E.
        level: Granularity level:
            - "sector": GICS sector level (default, ~11 sectors)
            - "industry": More granular industry level
        format: Output format:
            - "summary": Sector heatmap with performance + valuation
            - "full": Complete raw data from all endpoints
        use_cache: Use cached data when available (default: True).

    Returns:
        Sector overview with status field ("success" or "error").
    """
    _saved = sys.stdout
    sys.stdout = sys.stderr
    try:
        if symbols is not None:
            if sector:
                return {
                    "status": "error",
                    "error": (
                        "Cannot combine 'symbols' and 'sector' parameters. "
                        "Use 'symbols' for per-stock P/E comparison, or "
                        "'sector' for sector-level overview, but not both."
                    ),
                }
            return _fetch_symbol_pe_comparison(symbols, date, level, format, use_cache)
        return _fetch_sector_overview(date, sector, level, format, use_cache)
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
        }
    finally:
        sys.stdout = _saved


def _fetch_sector_overview(
    snapshot_date: Optional[str],
    sector_filter: Optional[str],
    level: str,
    format: str,
    use_cache: bool,
) -> dict:
    """Core logic for sector overview."""
    client = FMPClient()

    # Choose endpoints based on level
    if level == "industry":
        perf_endpoint = "industry_performance_snapshot"
        pe_endpoint = "industry_pe_snapshot"
        name_key = "industry"
    else:
        perf_endpoint = "sector_performance_snapshot"
        pe_endpoint = "sector_pe_snapshot"
        name_key = "sector"

    # Build fetch params — FMP requires a date param for snapshot endpoints
    if not snapshot_date:
        snapshot_date = _last_trading_day()
    fetch_params = {"date": snapshot_date}

    # Fetch performance and P/E snapshots
    perf_df = _safe_fetch(client, perf_endpoint, use_cache=use_cache, **fetch_params)
    pe_df = _safe_fetch(client, pe_endpoint, use_cache=use_cache, **fetch_params)

    # Determine the snapshot date from data (if not provided by user)
    display_date = snapshot_date
    if not display_date and not perf_df.empty and "date" in perf_df.columns:
        display_date = str(perf_df["date"].iloc[0])
    if not display_date:
        display_date = date.today().isoformat()

    # Handle empty data
    if perf_df.empty and pe_df.empty:
        result = {
            "status": "success",
            "date": display_date,
            "level": level,
            "sectors": [],
            "best": None,
            "worst": None,
            "count": 0,
            "note": "No sector data available for this date.",
        }
        if sector_filter:
            result["note"] = (
                f"No data found for {level} '{sector_filter}'. "
                f"Use get_sector_overview() without sector filter to see available {level}s."
            )
        return result

    # Full format: return raw data
    if format == "full":
        perf_records = perf_df.to_dict("records") if not perf_df.empty else []
        pe_records = pe_df.to_dict("records") if not pe_df.empty else []

        result = {
            "status": "success",
            "date": display_date,
            "level": level,
            "performance": perf_records,
            "valuation": pe_records,
            "row_count": max(len(perf_records), len(pe_records)),
        }

        if sector_filter:
            # Filter raw data to requested sector/industry
            sector_lower = sector_filter.lower()
            result["performance"] = [
                r for r in perf_records
                if r.get(name_key, r.get("sector", "")).lower() == sector_lower
            ]
            result["valuation"] = [
                r for r in pe_records
                if r.get(name_key, r.get("sector", "")).lower() == sector_lower
            ]
            result["row_count"] = max(
                len(result["performance"]), len(result["valuation"])
            )
            if result["row_count"] == 0:
                result["note"] = (
                    f"No data found for {level} '{sector_filter}'. "
                    f"Use get_sector_overview() without sector filter to see available {level}s."
                )

        return result

    # Summary format: merge performance + P/E
    sectors = _merge_sector_data(perf_df, pe_df, name_key)

    # Apply sector filter
    if sector_filter:
        sector_lower = sector_filter.lower()
        sectors = [s for s in sectors if s[name_key].lower() == sector_lower]

    # Sort by change % descending (treat None as 0 for sorting)
    sectors.sort(key=lambda s: s.get("change_pct") if s.get("change_pct") is not None else 0, reverse=True)

    # Compute best/worst
    best = None
    worst = None
    if sectors:
        best = {name_key: sectors[0][name_key], "change_pct": sectors[0]["change_pct"]}
        worst = {name_key: sectors[-1][name_key], "change_pct": sectors[-1]["change_pct"]}

    result = {
        "status": "success",
        "date": display_date,
        "level": level,
        "sectors": sectors,
        "best": best,
        "worst": worst,
        "count": len(sectors),
    }

    if sector_filter and not sectors:
        result["note"] = (
            f"No data found for {level} '{sector_filter}'. "
            f"Use get_sector_overview() without sector filter to see available {level}s."
        )

    return result


def _fetch_symbol_pe_comparison(
    symbols: list[str],
    snapshot_date: Optional[str],
    level: str,
    format: str,
    use_cache: bool,
) -> dict:
    """Compare per-symbol TTM P/E against sector/industry benchmark P/E."""
    normalized_symbols: list[str] = []
    for symbol in symbols:
        if symbol is None:
            continue
        normalized = str(symbol).strip().upper()
        if normalized:
            normalized_symbols.append(normalized)
    normalized_symbols = list(dict.fromkeys(normalized_symbols))

    if not normalized_symbols:
        return {
            "status": "error",
            "error": (
                "No valid symbols provided. Pass a non-empty list like "
                "symbols=['AAPL', 'MSFT']."
            ),
        }

    truncated_warning = None
    original_count = len(normalized_symbols)
    if original_count > MAX_COMPARISON_SYMBOLS:
        normalized_symbols = normalized_symbols[:MAX_COMPARISON_SYMBOLS]
        truncated_warning = (
            f"Received {original_count} unique symbols. Only first "
            f"{MAX_COMPARISON_SYMBOLS} were processed."
        )

    if level == "industry":
        benchmark_level = "industry"
        pe_endpoint = "industry_pe_snapshot"
    else:
        benchmark_level = "sector"
        pe_endpoint = "sector_pe_snapshot"

    if not snapshot_date:
        snapshot_date = _last_trading_day()

    client = FMPClient()
    pe_df = _safe_fetch(client, pe_endpoint, use_cache=use_cache, date=snapshot_date)

    benchmark_lookup: dict[str, tuple[str, float]] = {}
    if not pe_df.empty:
        for _, row in pe_df.iterrows():
            raw_name = row.get(benchmark_level, row.get("sector"))
            name = str(raw_name).strip() if raw_name is not None else ""
            if not name:
                continue
            pe_val = row.get("pe", row.get("peRatio"))
            if pe_val is None:
                continue
            try:
                pe_float = float(pe_val)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(pe_float):
                continue
            benchmark_lookup[name.lower()] = (name, round(pe_float, 2))

    symbol_results: dict[
        str,
        tuple[dict | None, dict | None, list[dict] | None, dict | None],
    ] = {}
    failed_symbols_set: set[str] = set()

    with ThreadPoolExecutor(max_workers=min(len(normalized_symbols), 5)) as executor:
        futures = {
            executor.submit(_fetch_symbol_data, client, symbol, use_cache): symbol
            for symbol in normalized_symbols
        }
        for future in as_completed(futures):
            requested_symbol = futures[future]
            try:
                symbol, profile, ratios, estimates, income_statement, error = future.result()
            except Exception:
                failed_symbols_set.add(requested_symbol)
                continue

            if error:
                failed_symbols_set.add(symbol)
            else:
                symbol_results[symbol] = (profile, ratios, estimates, income_statement)

    failed_symbols = [s for s in normalized_symbols if s in failed_symbols_set]
    if len(failed_symbols) == len(normalized_symbols):
        return {
            "status": "error",
            "error": "Failed to fetch data for all requested symbols.",
            "failed_symbols": failed_symbols,
        }

    comparisons: list[dict] = []
    for symbol in normalized_symbols:
        if symbol in failed_symbols_set:
            continue

        profile, ratios, estimates, income_statement = symbol_results.get(
            symbol,
            (None, None, None, None),
        )
        profile = profile or {}
        ratios = ratios or {}
        estimates = estimates or []
        income_statement = income_statement or {}

        company_name = profile.get("companyName")
        sector_name = str(profile.get("sector") or "").strip() or None
        industry_name = str(profile.get("industry") or "").strip() or None

        benchmark_name = industry_name if benchmark_level == "industry" else sector_name
        benchmark_match = (
            benchmark_lookup.get(benchmark_name.lower()) if benchmark_name else None
        )
        benchmark_pe = benchmark_match[1] if benchmark_match else None
        benchmark_name = benchmark_match[0] if benchmark_match else None

        last_reported_fiscal_date = (
            str(income_statement.get("date"))[:10]
            if income_statement.get("date")
            else None
        )
        forward_pe_result = compute_forward_pe(
            parse_fmp_float(profile.get("price")),
            estimates,
            last_reported_fiscal_date,
        )

        stock_pe_raw = ratios.get("priceToEarningsRatioTTM")
        fallback_ttm_pe: float | None = None
        if stock_pe_raw is not None:
            try:
                stock_pe_float = float(stock_pe_raw)
                if math.isfinite(stock_pe_float) and stock_pe_float > 0:
                    fallback_ttm_pe = round(stock_pe_float, 2)
            except (TypeError, ValueError):
                fallback_ttm_pe = None

        stock_pe = forward_pe_result.get("forward_pe")
        if stock_pe is None:
            stock_pe = fallback_ttm_pe

        pe_source = (
            "forward"
            if forward_pe_result.get("forward_pe") is not None
            else "ttm"
            if fallback_ttm_pe is not None
            else forward_pe_result.get("pe_source")
        )

        premium_pct = _compute_pe_premium(stock_pe, benchmark_pe)
        verdict = _classify_verdict(
            stock_pe,
            benchmark_pe,
            premium_pct,
            forward_pe_result.get("forward_pe")
            if forward_pe_result.get("forward_pe") is not None
            else stock_pe_raw,
        )

        comparison = {
            "symbol": symbol,
            "name": company_name,
            "sector": sector_name,
            "industry": industry_name,
            "stock_pe": stock_pe,
            "pe_source": pe_source,
            "benchmark_pe": benchmark_pe,
            "benchmark_pe_source": "ttm",
            "benchmark_name": benchmark_name,
            "premium_pct": premium_pct,
            "verdict": verdict,
        }

        if format == "full":
            comparison["profile_raw"] = profile
            comparison["ratios_raw"] = ratios

        comparisons.append(comparison)

    summary = {
        "above_count": 0,
        "below_count": 0,
        "at_par_count": 0,
        "no_benchmark_count": 0,
        "negative_earnings_count": 0,
        "avg_premium_pct": None,
    }

    premium_values = []
    for comparison in comparisons:
        verdict = comparison["verdict"]
        if verdict == "above":
            summary["above_count"] += 1
        elif verdict == "below":
            summary["below_count"] += 1
        elif verdict == "at_par":
            summary["at_par_count"] += 1
        elif verdict == "no_benchmark":
            summary["no_benchmark_count"] += 1
        elif verdict == "negative_earnings":
            summary["negative_earnings_count"] += 1

        premium = comparison.get("premium_pct")
        if premium is not None:
            premium_values.append(float(premium))

    if premium_values:
        summary["avg_premium_pct"] = round(sum(premium_values) / len(premium_values), 2)

    result = {
        "status": "success",
        "date": snapshot_date,
        "level": level,
        "benchmark_level": benchmark_level,
        "mode": "comparison",
        "comparisons": comparisons,
        "summary": summary,
        "failed_symbols": failed_symbols,
        "count": len(comparisons),
    }
    if truncated_warning:
        result["truncated_warning"] = truncated_warning
    return result


def _fetch_symbol_data(
    client: FMPClient,
    symbol: str,
    use_cache: bool,
) -> tuple[str, dict | None, dict | None, list[dict] | None, dict | None, str | None]:
    """Fetch profile, ratios, and best-effort estimate context for a symbol."""
    try:
        profile_df = client.fetch("profile", symbol=symbol, use_cache=use_cache)
        ratios_df = client.fetch("ratios_ttm", symbol=symbol, use_cache=use_cache)
    except Exception as e:
        return (symbol, None, None, None, None, str(e))

    estimates: list[dict] | None = None
    try:
        estimates_df = client.fetch(
            "analyst_estimates",
            symbol=symbol,
            period="annual",
            limit=4,
            use_cache=use_cache,
        )
        if estimates_df is not None and hasattr(estimates_df, "empty") and not estimates_df.empty:
            estimates = estimates_df.to_dict("records")
        else:
            estimates = []
    except Exception:
        estimates = None

    income_statement: dict | None = None
    try:
        income_statement_df = client.fetch(
            "income_statement",
            symbol=symbol,
            period="quarter",
            limit=1,
            use_cache=use_cache,
        )
        income_statement = first_dataframe_record(income_statement_df) or None
    except Exception:
        income_statement = None

    profile = first_dataframe_record(profile_df) or None
    ratios = first_dataframe_record(ratios_df) or None
    return (symbol, profile, ratios, estimates, income_statement, None)


def _compute_pe_premium(stock_pe, benchmark_pe):
    """Compute stock P/E premium/discount vs benchmark."""
    if stock_pe is None or benchmark_pe is None:
        return None
    try:
        stock_pe_f, benchmark_pe_f = float(stock_pe), float(benchmark_pe)
    except (ValueError, TypeError):
        return None
    if (
        benchmark_pe_f == 0
        or not math.isfinite(benchmark_pe_f)
        or not math.isfinite(stock_pe_f)
    ):
        return None
    return round((stock_pe_f - benchmark_pe_f) / benchmark_pe_f * 100, 2)


def _classify_verdict(
    stock_pe: float | None,
    benchmark_pe: float | None,
    premium_pct: float | None,
    stock_pe_raw=None,
) -> str:
    """Classify valuation verdict from stock and benchmark P/E."""
    if stock_pe_raw is not None:
        try:
            stock_pe_raw_f = float(stock_pe_raw)
            if math.isfinite(stock_pe_raw_f) and stock_pe_raw_f <= 0:
                return "negative_earnings"
        except (TypeError, ValueError):
            pass

    if benchmark_pe is None:
        return "no_benchmark"
    if stock_pe is None or premium_pct is None:
        return "no_data"
    if premium_pct > PE_PREMIUM_THRESHOLD_PCT:
        return "above"
    if premium_pct < -PE_PREMIUM_THRESHOLD_PCT:
        return "below"
    return "at_par"


def _merge_sector_data(
    perf_df: pd.DataFrame,
    pe_df: pd.DataFrame,
    name_key: str,
) -> list[dict]:
    """Merge performance and P/E data on sector/industry name.

    Args:
        perf_df: Performance snapshot DataFrame.
        pe_df: P/E snapshot DataFrame.
        name_key: Column name to merge on ("sector" or "industry").

    Returns:
        List of dicts with sector/industry name, change_pct, and pe_ratio.
    """
    sectors = []

    # Build P/E lookup by name (case-insensitive)
    pe_lookup: dict[str, float] = {}
    if not pe_df.empty:
        for _, row in pe_df.iterrows():
            name = str(row.get(name_key, row.get("sector", "")))
            pe_val = row.get("pe", row.get("peRatio"))
            if name and pe_val is not None:
                try:
                    pe_lookup[name.lower()] = round(float(pe_val), 2)
                except (ValueError, TypeError):
                    pass

    # Build merged records from performance data
    if not perf_df.empty:
        for _, row in perf_df.iterrows():
            name = str(row.get(name_key, row.get("sector", "")))
            if not name:
                continue
            change_pct_val = row.get("changesPercentage", row.get("averageChange", row.get("change_pct")))
            try:
                change_pct = round(float(change_pct_val), 4) if change_pct_val is not None else None
            except (ValueError, TypeError):
                change_pct = None

            entry = {
                name_key: name,
                "change_pct": change_pct,
                "pe_ratio": pe_lookup.get(name.lower()),
            }
            sectors.append(entry)
    elif pe_lookup:
        # Only P/E data available, no performance
        for name_lower, pe_val in pe_lookup.items():
            # Try to recover original case from pe_df
            original_name = name_lower
            if not pe_df.empty:
                for _, row in pe_df.iterrows():
                    rn = str(row.get(name_key, row.get("sector", "")))
                    if rn.lower() == name_lower:
                        original_name = rn
                        break
            sectors.append({
                name_key: original_name,
                "change_pct": None,
                "pe_ratio": pe_val,
            })

    return sectors


# ---------------------------------------------------------------------------
# get_market_context
# ---------------------------------------------------------------------------

_INDEX_NAMES = {
    "^GSPC": "S&P 500",
    "^DJI": "Dow Jones",
    "^IXIC": "Nasdaq",
    "^RUT": "Russell 2000",
}

# Include param values = response dict keys (1:1 mapping)
MARKET_CONTEXT_SECTIONS = ["indices", "sectors", "gainers", "losers", "actives", "events"]


def _safe_fetch_records(
    client: FMPClient,
    endpoint_name: str,
    use_cache: bool = True,
    **params,
) -> dict:
    """Fetch from FMP via client.fetch (cached), return structured result."""
    try:
        df = client.fetch(endpoint_name, use_cache=use_cache, **params)
        if df is not None and not df.empty:
            records = df.to_dict("records")
            for rec in records:
                for key, val in rec.items():
                    if val is not None and pd.isna(val):
                        rec[key] = None
            return {"ok": True, "data": records, "error": None}
        return {"ok": True, "data": [], "error": None}
    except FMPEmptyResponseError:
        return {"ok": True, "data": [], "error": None}
    except Exception as e:
        return {"ok": False, "data": [], "error": str(e)}


def _safe_float(val, default=None):
    """Parse numeric value with fallback."""
    if val is None:
        return default
    if isinstance(val, str):
        val = val.strip().replace("%", "").replace(",", "")
        if not val:
            return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _get_change_pct(record: dict):
    """Extract change percentage from FMP record (field name varies)."""
    for key in ["changesPercentage", "changePercentage", "averageChange"]:
        val = _safe_float(record.get(key))
        if val is not None:
            return round(val, 4)
    return None


def _first_non_null(record: dict, keys: list[str]):
    """Return first non-null key value from a record."""
    for key in keys:
        value = record.get(key)
        if value is not None and value != "":
            return value
    return None


def _extract_as_of(records: list[dict]) -> Optional[str]:
    """Extract source timestamp/date when available."""
    for record in records:
        val = _first_non_null(
            record,
            ["timestamp", "lastUpdated", "date", "publishedDate", "datetime"],
        )
        if val is not None:
            return str(val)
    return None



def _normalize_sectors(records: list[dict]) -> tuple[list[dict], Optional[str]]:
    """Normalize sector performance records."""
    sectors = []
    for rec in records:
        sector_name = rec.get("sector") or rec.get("name")
        if not sector_name:
            continue
        sectors.append({
            "sector": str(sector_name),
            "change_pct": _get_change_pct(rec),
        })

    sectors.sort(
        key=lambda s: (
            s.get("change_pct") is None,
            -(s.get("change_pct") if s.get("change_pct") is not None else 0),
        ),
    )
    return sectors, _extract_as_of(records)


def _normalize_movers(
    records: list[dict],
    include_price: bool,
    include_volume: bool,
) -> tuple[list[dict], Optional[str]]:
    """Normalize mover endpoint records."""
    movers = []
    for rec in records:
        symbol = str(rec.get("symbol") or "").strip()
        if not symbol:
            continue

        item = {
            "symbol": symbol,
            "name": rec.get("name") or rec.get("companyName") or "",
            "change_pct": _get_change_pct(rec),
        }
        if include_price:
            item["price"] = _safe_float(_first_non_null(rec, ["price", "lastPrice", "last"]))
        if include_volume:
            vol = _safe_float(rec.get("volume"))
            item["volume"] = int(vol) if vol is not None else None

        movers.append(item)

    return movers, _extract_as_of(records)


def _normalize_events(records: list[dict]) -> tuple[list[dict], Optional[str]]:
    """Normalize economic calendar records."""
    events = []
    for rec in records:
        event_name = rec.get("event") or rec.get("name")
        event_date = str(rec.get("date") or "")[:10]
        if not event_name and not event_date:
            continue
        events.append({
            "event": event_name or "",
            "date": event_date,
            "estimate": rec.get("estimate"),
            "impact": rec.get("impact") or "",
        })

    events.sort(key=lambda e: e.get("date") or "")
    return events, _extract_as_of(records)


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


def get_market_context(
    include: Optional[list[str]] = None,
    format: Literal["full", "summary"] = "summary",
    use_cache: bool = True,
) -> dict:
    """
    Get a multi-section market context snapshot in one call.

    Sections available: indices, sectors, gainers, losers, actives, events.
    """
    _saved = sys.stdout
    sys.stdout = sys.stderr
    try:
        invalid_sections = []

        if include is None:
            requested_sections = MARKET_CONTEXT_SECTIONS.copy()
        else:
            requested_sections = []
            for section in include:
                if not isinstance(section, str):
                    invalid_sections.append(str(section))
                    continue
                section_name = section.strip().lower()
                if section_name in MARKET_CONTEXT_SECTIONS:
                    if section_name not in requested_sections:
                        requested_sections.append(section_name)
                else:
                    invalid_sections.append(section)

            if not requested_sections:
                return {
                    "status": "error",
                    "error": (
                        "All requested sections are invalid. "
                        f"Valid sections: {', '.join(MARKET_CONTEXT_SECTIONS)}"
                    ),
                    "invalid_sections": invalid_sections,
                    "requested_sections": [],
                }

        today = date.today().isoformat()
        generated_at = datetime.utcnow().replace(microsecond=0).isoformat()

        client = FMPClient()
        warnings = []
        source_status = {}
        response = {
            "status": "success",
            "date": today,
            "generated_at": generated_at,
            "requested_sections": requested_sections,
        }
        if invalid_sections:
            response["invalid_sections"] = invalid_sections

        if "indices" in requested_sections:
            indices = []
            as_of = None
            for symbol, name in _INDEX_NAMES.items():
                result = _safe_fetch_records(
                    client, "historical_price_eod", use_cache=use_cache,
                    symbol=symbol, limit=1,
                )
                if result["ok"] and result["data"]:
                    rec = result["data"][0]
                    indices.append({
                        "symbol": symbol,
                        "name": name,
                        "price": _safe_float(rec.get("close")),
                        "change_pct": _safe_float(rec.get("changePercent")),
                    })
                    if as_of is None:
                        as_of = str(rec.get("date", ""))
            if indices:
                response["indices"] = indices
                _add_source_status(source_status, "indices", True, len(indices), as_of)
            else:
                response["indices"] = []
                _add_source_status(source_status, "indices", False, 0)
                warnings.append("indices: no data returned from historical_price_eod")

        if "sectors" in requested_sections:
            # FMP sector snapshot requires a date param; default to last trading day
            sector_date = _last_trading_day()
            result = _safe_fetch_records(
                client,
                "sector_performance_snapshot",
                use_cache=use_cache,
                date=sector_date,
            )
            if result["ok"]:
                sectors, as_of = _normalize_sectors(result["data"])
                response["sectors"] = sectors
                _add_source_status(source_status, "sectors", True, len(sectors), as_of)
            else:
                response["sectors"] = []
                _add_source_status(source_status, "sectors", False, 0)
                warnings.append(f"sectors: {result['error']}")

        if "gainers" in requested_sections:
            result = _safe_fetch_records(client, "biggest_gainers", use_cache=use_cache)
            if result["ok"]:
                gainers, as_of = _normalize_movers(
                    result["data"],
                    include_price=True,
                    include_volume=False,
                )
                gainers.sort(
                    key=lambda g: g.get("change_pct") if g.get("change_pct") is not None else float("-inf"),
                    reverse=True,
                )
                if format == "summary":
                    gainers = gainers[:5]
                else:
                    gainers = gainers[:20]
                response["gainers"] = gainers
                _add_source_status(source_status, "gainers", True, len(gainers), as_of)
            else:
                response["gainers"] = []
                _add_source_status(source_status, "gainers", False, 0)
                warnings.append(f"gainers: {result['error']}")

        if "losers" in requested_sections:
            result = _safe_fetch_records(client, "biggest_losers", use_cache=use_cache)
            if result["ok"]:
                losers, as_of = _normalize_movers(
                    result["data"],
                    include_price=True,
                    include_volume=False,
                )
                losers.sort(
                    key=lambda l: l.get("change_pct") if l.get("change_pct") is not None else float("inf"),
                )
                if format == "summary":
                    losers = losers[:5]
                else:
                    losers = losers[:20]
                response["losers"] = losers
                _add_source_status(source_status, "losers", True, len(losers), as_of)
            else:
                response["losers"] = []
                _add_source_status(source_status, "losers", False, 0)
                warnings.append(f"losers: {result['error']}")

        if "actives" in requested_sections:
            result = _safe_fetch_records(client, "most_actives", use_cache=use_cache)
            if result["ok"]:
                # FMP most_actives returns price+change but no volume field
                actives, as_of = _normalize_movers(
                    result["data"],
                    include_price=True,
                    include_volume=False,
                )
                if format == "summary":
                    actives = actives[:5]
                else:
                    actives = actives[:20]
                response["actives"] = actives
                _add_source_status(source_status, "actives", True, len(actives), as_of)
            else:
                response["actives"] = []
                _add_source_status(source_status, "actives", False, 0)
                warnings.append(f"actives: {result['error']}")

        if "events" in requested_sections:
            from_date = date.today().isoformat()
            to_date = (date.today() + timedelta(days=7)).isoformat()
            result = _safe_fetch_records(
                client,
                "economic_calendar",
                use_cache=use_cache,
                from_date=from_date,
                to_date=to_date,
            )
            if result["ok"]:
                events, as_of = _normalize_events(result["data"])
                # Filter to US events by default (same as get_economic_data)
                events = [
                    e for e in events
                    if str(e.get("country") or "").upper() == "US"
                ]
                if format == "summary":
                    events = [e for e in events if str(e.get("impact", "")).lower() == "high"][:5]
                response["events"] = events
                _add_source_status(source_status, "events", True, len(events), as_of)
            else:
                response["events"] = []
                _add_source_status(source_status, "events", False, 0)
                warnings.append(f"events: {result['error']}")

        response["source_status"] = source_status
        response["warnings"] = warnings

        if requested_sections and all(not source_status[s]["ok"] for s in requested_sections):
            return {
                "status": "error",
                "error": "Failed to fetch all requested sections.",
                "date": today,
                "generated_at": generated_at,
                "requested_sections": requested_sections,
                "source_status": source_status,
                "warnings": warnings,
                **({"invalid_sections": invalid_sections} if invalid_sections else {}),
            }

        return response

    except Exception as e:
        return {"status": "error", "error": str(e)}
    finally:
        sys.stdout = _saved
