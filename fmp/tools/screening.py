"""
MCP Tool: screen_stocks

Exposes stock screening as an MCP tool for AI invocation.

Usage (from Claude):
    "Find large-cap tech stocks" -> screen_stocks(sector="Technology", market_cap_min=10000000000)
    "Low beta dividend stocks" -> screen_stocks(beta_max=0.8, dividend_min=2.0)

Architecture note:
- Standalone tool (no portfolio loading, no user context required)
- Wraps FMP company-screener endpoint
- stdout is redirected to stderr to protect MCP JSON-RPC channel from stray prints
"""

import sys
from typing import Optional, Literal

from ..client import FMPClient


# === Constants ===

# MCP param name -> FMP API param name
_SCREENER_PARAM_MAP = {
    "market_cap_min": "marketCapMoreThan",
    "market_cap_max": "marketCapLowerThan",
    "price_min": "priceMoreThan",
    "price_max": "priceLowerThan",
    "dividend_min": "dividendMoreThan",
    "dividend_max": "dividendLowerThan",
    "beta_min": "betaMoreThan",
    "beta_max": "betaLowerThan",
    "volume_min": "volumeMoreThan",
    "volume_max": "volumeLowerThan",
    "is_etf": "isEtf",
    "is_fund": "isFund",
}

# Pass-through params (same name in MCP and FMP)
_SCREENER_PASSTHROUGH = ["sector", "industry", "country", "exchange", "limit"]

# Fields to extract for summary format
_SUMMARY_FIELDS = [
    "symbol", "companyName", "sector", "industry",
    "marketCap", "price", "beta", "volume",
    "lastAnnualDividend", "exchange", "country",
]

# Friendly key names for summary output
_SUMMARY_KEY_MAP = {
    "companyName": "name",
    "marketCap": "market_cap",
    "lastAnnualDividend": "dividend",
}


# === Helpers ===

def _build_screener_params(
    market_cap_min: Optional[float] = None,
    market_cap_max: Optional[float] = None,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    dividend_min: Optional[float] = None,
    dividend_max: Optional[float] = None,
    beta_min: Optional[float] = None,
    beta_max: Optional[float] = None,
    volume_min: Optional[float] = None,
    volume_max: Optional[float] = None,
    is_etf: Optional[bool] = None,
    is_fund: Optional[bool] = None,
    sector: Optional[str] = None,
    industry: Optional[str] = None,
    country: Optional[str] = None,
    exchange: Optional[str] = None,
    limit: int = 50,
) -> dict:
    """Map user-friendly MCP params to FMP screener API params.

    Only includes non-None values. Always sets isActivelyTrading=True.
    """
    local_vals = {
        "market_cap_min": market_cap_min,
        "market_cap_max": market_cap_max,
        "price_min": price_min,
        "price_max": price_max,
        "dividend_min": dividend_min,
        "dividend_max": dividend_max,
        "beta_min": beta_min,
        "beta_max": beta_max,
        "volume_min": volume_min,
        "volume_max": volume_max,
        "is_etf": is_etf,
        "is_fund": is_fund,
        "sector": sector,
        "industry": industry,
        "country": country,
        "exchange": exchange,
        "limit": limit,
    }

    params = {}

    # Mapped params (snake_case -> camelCase)
    for mcp_name, fmp_name in _SCREENER_PARAM_MAP.items():
        value = local_vals.get(mcp_name)
        if value is not None:
            params[fmp_name] = value

    # Pass-through params
    for name in _SCREENER_PASSTHROUGH:
        value = local_vals.get(name)
        if value is not None:
            params[name] = value

    # Always filter to actively trading stocks
    params["isActivelyTrading"] = True

    return params


def _format_screener_summary(results: list[dict]) -> list[dict]:
    """Extract key fields from raw screener results for summary format."""
    summary = []
    for item in results:
        row = {}
        for field in _SUMMARY_FIELDS:
            value = item.get(field)
            key = _SUMMARY_KEY_MAP.get(field, field)
            row[key] = value
        summary.append(row)
    return summary


def _build_filters_applied(**kwargs) -> dict:
    """Build a dict of non-None filter params for response context."""
    return {k: v for k, v in kwargs.items() if v is not None and k != "limit" and k != "format"}


# === Tool ===

def screen_stocks(
    sector: Optional[str] = None,
    industry: Optional[str] = None,
    market_cap_min: Optional[float] = None,
    market_cap_max: Optional[float] = None,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    dividend_min: Optional[float] = None,
    dividend_max: Optional[float] = None,
    beta_min: Optional[float] = None,
    beta_max: Optional[float] = None,
    volume_min: Optional[float] = None,
    volume_max: Optional[float] = None,
    country: Optional[str] = None,
    exchange: Optional[str] = None,
    is_etf: Optional[bool] = None,
    is_fund: Optional[bool] = None,
    limit: int = 50,
    format: Literal["full", "summary"] = "summary",
) -> dict:
    """
    Screen stocks by fundamental criteria (sector, market cap, beta, dividend, etc.).

    Args:
        sector: Sector filter (e.g., "Technology", "Healthcare").
        industry: Industry filter (e.g., "Software", "Biotechnology").
        market_cap_min: Minimum market capitalization in USD.
        market_cap_max: Maximum market capitalization in USD.
        price_min: Minimum stock price.
        price_max: Maximum stock price.
        dividend_min: Minimum annual dividend per share.
        dividend_max: Maximum annual dividend per share.
        beta_min: Minimum beta (market sensitivity).
        beta_max: Maximum beta.
        volume_min: Minimum average daily volume.
        volume_max: Maximum average daily volume.
        country: Country filter (e.g., "US", "GB").
        exchange: Exchange filter (e.g., "NASDAQ", "NYSE").
        is_etf: Set to true to screen ETFs only, false for stocks only.
        is_fund: Set to true to screen funds only, false to exclude funds.
        limit: Maximum number of results (default: 50).
        format: "summary" for key fields, "full" for all fields.

    Returns:
        dict: Screening results with status field ("success" or "error").
    """
    _saved = sys.stdout
    sys.stdout = sys.stderr
    try:
        # Validate: at least one filter must be provided
        filter_params = {
            "sector": sector, "industry": industry,
            "market_cap_min": market_cap_min, "market_cap_max": market_cap_max,
            "price_min": price_min, "price_max": price_max,
            "dividend_min": dividend_min, "dividend_max": dividend_max,
            "beta_min": beta_min, "beta_max": beta_max,
            "volume_min": volume_min, "volume_max": volume_max,
            "country": country, "exchange": exchange,
            "is_etf": is_etf, "is_fund": is_fund,
        }
        if not any(v is not None for v in filter_params.values()):
            return {
                "status": "error",
                "error": (
                    "At least one filter is required. Provide sector, industry, "
                    "market_cap_min/max, beta_min/max, price_min/max, dividend_min/max, "
                    "volume_min/max, country, exchange, is_etf, or is_fund."
                ),
            }

        # Build FMP params
        fmp_params = _build_screener_params(
            market_cap_min=market_cap_min,
            market_cap_max=market_cap_max,
            price_min=price_min,
            price_max=price_max,
            dividend_min=dividend_min,
            dividend_max=dividend_max,
            beta_min=beta_min,
            beta_max=beta_max,
            volume_min=volume_min,
            volume_max=volume_max,
            is_etf=is_etf,
            is_fund=is_fund,
            sector=sector,
            industry=industry,
            country=country,
            exchange=exchange,
            limit=limit,
        )

        # Fetch raw JSON from FMP
        fmp = FMPClient()
        results = fmp.fetch_raw("company_screener", **fmp_params)

        # Ensure we have a list
        if not isinstance(results, list):
            results = [results] if results else []

        # Build filters_applied for context
        filters_applied = _build_filters_applied(**filter_params)

        # Handle empty results
        if not results:
            return {
                "status": "success",
                "result_count": 0,
                "filters_applied": filters_applied,
                "results": [],
                "note": "No stocks matched your criteria. Try broadening your filters.",
            }

        # Format response
        if format == "summary":
            formatted_results = _format_screener_summary(results)
        else:
            formatted_results = results

        return {
            "status": "success",
            "result_count": len(formatted_results),
            "filters_applied": filters_applied,
            "results": formatted_results,
        }

    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
        }
    finally:
        sys.stdout = _saved
