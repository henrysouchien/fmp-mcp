"""
MCP Tools: get_news, get_events_calendar

Exposes FMP news and event calendar data as MCP tools for AI invocation.
"""

import sys
from datetime import datetime, timedelta
from typing import Literal, Optional

from ..client import FMPClient


# --- News source quality tiers ---
# Tier 1: Wire services — official company press releases
_TIER1_WIRE = {
    "businesswire.com",
    "prnewswire.com",
    "globenewswire.com",
    "accesswire.com",
}

# Tier 2: Credible financial journalism
_TIER2_JOURNALISM = {
    "wsj.com",
    "cnbc.com",
    "bloomberg.com",
    "reuters.com",
    "ft.com",
    "marketwatch.com",
    "investors.com",
    "barrons.com",
}

_QUALITY_TIERS = {
    "all": None,  # No filtering
    "trusted": _TIER1_WIRE | _TIER2_JOURNALISM,
    "wire": _TIER1_WIRE,
    "journalism": _TIER2_JOURNALISM,
}


def _filter_by_quality(articles: list[dict], quality: str) -> list[dict]:
    """Filter articles to only include sources from the specified quality tier."""
    allowed = _QUALITY_TIERS.get(quality)
    if allowed is None:
        return articles
    return [
        a for a in articles
        if (a.get("site") or a.get("source") or "").lower() in allowed
    ]


# Maps event_type to FMP endpoint name
_CALENDAR_ENDPOINTS = {
    "earnings": "earnings_calendar",
    "dividends": "dividends_calendar",
    "splits": "splits_calendar",
    "ipos": "ipos_calendar",
}


def _fetch_calendar(fmp: FMPClient, endpoint_name: str, from_date: str, to_date: str) -> list[dict]:
    """Fetch a single calendar endpoint, returning list of events."""
    try:
        raw = fmp.fetch_raw(endpoint_name, from_date=from_date, to_date=to_date)
        if isinstance(raw, list):
            return raw
        if raw:
            return [raw]
        return []
    except Exception:
        # Individual calendar fetch failure should not break "all" mode
        return []


def _summarize_event(event: dict, event_type: str) -> dict:
    """Extract summary fields for an event based on its type."""
    base = {
        "event_type": event_type,
        "symbol": event.get("symbol", ""),
        "date": event.get("date", ""),
    }

    if event_type == "earnings":
        base["eps_estimated"] = event.get("epsEstimated")
        base["eps_actual"] = event.get("eps")
        base["revenue_estimated"] = event.get("revenueEstimated")
        base["revenue_actual"] = event.get("revenue")
    elif event_type == "dividends":
        base["dividend"] = event.get("dividend") or event.get("adjDividend")
        base["record_date"] = event.get("recordDate", "")
        base["payment_date"] = event.get("paymentDate", "")
    elif event_type == "splits":
        base["numerator"] = event.get("numerator")
        base["denominator"] = event.get("denominator")
    elif event_type == "ipos":
        base["company"] = event.get("company", "")
        base["price_range"] = event.get("priceRange", "")
        base["shares"] = event.get("shares")

    return base


def get_news(
    symbols: Optional[str] = None,
    mode: Literal["stock", "general", "press"] = "stock",
    limit: int = 10,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    format: Literal["summary", "full"] = "summary",
    quality: Literal["all", "trusted", "wire", "journalism"] = "trusted",
) -> dict:
    """Fetch news articles for stocks or the broad market."""
    _saved = sys.stdout
    sys.stdout = sys.stderr
    try:
        limit = max(1, min(50, limit))
        # Over-fetch when filtering so we can still hit the requested limit
        fetch_limit = limit * 3 if quality != "all" else limit

        if mode in ("stock", "press") and not symbols:
            return {
                "status": "error",
                "error": (
                    f"symbols is required for mode='{mode}'. "
                    "Specify symbols directly (e.g., symbols='AAPL,MSFT')."
                ),
            }

        fmp = FMPClient()
        fetch_kwargs = {"limit": fetch_limit}
        if from_date:
            fetch_kwargs["from_date"] = from_date
        if to_date:
            fetch_kwargs["to_date"] = to_date

        if mode == "general":
            raw = fmp.fetch_raw("news_general", **fetch_kwargs)
        elif mode == "press":
            raw = fmp.fetch_raw("news_press_releases", symbols=symbols, **fetch_kwargs)
        else:
            raw = fmp.fetch_raw("news_stock", symbols=symbols, **fetch_kwargs)

        articles = raw if isinstance(raw, list) else [raw] if raw else []
        articles = _filter_by_quality(articles, quality)
        articles = articles[:limit]

        if format == "summary":
            formatted_articles = []
            for article in articles:
                snippet = (article.get("text") or "")[:200]
                if len(article.get("text") or "") > 200:
                    snippet += "..."
                formatted_articles.append({
                    "title": article.get("title", ""),
                    "date": (article.get("publishedDate") or "")[:10],
                    "source": article.get("site") or article.get("source", ""),
                    "symbol": article.get("symbol", ""),
                    "snippet": snippet,
                    "url": article.get("url", ""),
                })
            articles_out = formatted_articles
        else:
            articles_out = articles

        return {
            "status": "success",
            "mode": mode,
            "symbols": symbols or "",
            "article_count": len(articles_out),
            "articles": articles_out,
        }

    except Exception as e:
        return {"status": "error", "error": str(e)}
    finally:
        sys.stdout = _saved


def get_events_calendar(
    event_type: Literal["earnings", "dividends", "splits", "ipos", "all"] = "earnings",
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    symbols: Optional[str] = None,
    limit: Optional[int] = None,
    format: Literal["summary", "full"] = "summary",
) -> dict:
    """Fetch upcoming corporate events: earnings, dividends, splits, or IPOs."""
    _saved = sys.stdout
    sys.stdout = sys.stderr
    try:
        today = datetime.now()
        if not from_date:
            from_date = today.strftime("%Y-%m-%d")
        if not to_date:
            to_date = (today + timedelta(days=30)).strftime("%Y-%m-%d")

        try:
            dt_from = datetime.strptime(from_date, "%Y-%m-%d")
            dt_to = datetime.strptime(to_date, "%Y-%m-%d")
            if (dt_to - dt_from).days > 90:
                return {
                    "status": "error",
                    "error": "Date range exceeds 90-day maximum. Narrow the from_date/to_date window.",
                }
            if dt_to < dt_from:
                return {
                    "status": "error",
                    "error": "to_date must be after from_date.",
                }
        except ValueError:
            return {
                "status": "error",
                "error": "Invalid date format. Use YYYY-MM-DD.",
            }

        if event_type == "all" and not symbols and limit is None:
            limit = 20
        elif limit is None:
            limit = 50

        limit = max(1, min(500, int(limit)))

        symbol_filter = None
        if symbols:
            symbol_filter = {s.strip().upper() for s in symbols.split(",") if s.strip()}

        fmp = FMPClient()
        all_events = []

        if event_type == "all":
            for etype, endpoint_name in _CALENDAR_ENDPOINTS.items():
                events = _fetch_calendar(fmp, endpoint_name, from_date, to_date)
                for evt in events:
                    evt["_event_type"] = etype
                all_events.extend(events)
        else:
            endpoint_name = _CALENDAR_ENDPOINTS[event_type]
            events = _fetch_calendar(fmp, endpoint_name, from_date, to_date)
            for evt in events:
                evt["_event_type"] = event_type
            all_events = events

        if symbol_filter:
            all_events = [
                e for e in all_events
                if (e.get("symbol") or "").upper() in symbol_filter
            ]

        all_events.sort(key=lambda e: e.get("date", ""))

        total_events = len(all_events)
        all_events = all_events[:limit]

        if format == "summary":
            formatted_events = [
                _summarize_event(evt, evt.pop("_event_type", event_type))
                for evt in all_events
            ]
        else:
            formatted_events = []
            for evt in all_events:
                etype = evt.pop("_event_type", event_type)
                evt["event_type"] = etype
                formatted_events.append(evt)

        result = {
            "status": "success",
            "event_type": event_type,
            "from_date": from_date,
            "to_date": to_date,
            "event_count": len(formatted_events),
            "events": formatted_events,
        }
        if total_events > limit:
            result["total_available"] = total_events
            result["truncated"] = True
        return result

    except Exception as e:
        return {"status": "error", "error": str(e)}
    finally:
        sys.stdout = _saved
