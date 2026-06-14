"""Guard rails for FMP-provided beta values."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any


EXTREME_BETA_ABS_THRESHOLD = 4.0
RECENT_IPO_DAYS = 365 * 2


def _to_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric != numeric:
        return None
    return numeric


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None


def build_beta_warning(row: dict[str, Any], *, as_of: date | None = None) -> str | None:
    """Return a human-readable warning when FMP beta is likely unreliable."""
    if not isinstance(row, dict):
        return None

    reasons: list[str] = []
    beta = _to_float(row.get("beta"))
    if beta is not None and abs(beta) > EXTREME_BETA_ABS_THRESHOLD:
        reasons.append(
            f"absolute beta {beta:g} exceeds {EXTREME_BETA_ABS_THRESHOLD:g}"
        )

    ipo_date = _parse_date(row.get("ipoDate"))
    if ipo_date is not None:
        today = as_of or date.today()
        age_days = (today - ipo_date).days
        if age_days < RECENT_IPO_DAYS:
            reasons.append(
                f"IPO date {ipo_date.isoformat()} is less than 2 years old"
            )

    if not reasons:
        return None
    return (
        "FMP beta may be unreliable because "
        + "; ".join(reasons)
        + ". Treat this as an upstream FMP data-quality flag, not a computed risk beta."
    )


def add_beta_warning(row: dict[str, Any]) -> dict[str, Any]:
    """Return a shallow copy with beta_warning populated when applicable."""
    enriched = dict(row or {})
    warning = build_beta_warning(enriched)
    if warning:
        enriched["beta_warning"] = warning
    else:
        enriched.pop("beta_warning", None)
    return enriched


BETA_FILTER_WARNING = (
    "FMP applies beta_min/beta_max filters server-side using its pre-computed beta. "
    "That beta can be unreliable for recent IPOs or extreme beta values; review "
    "per-result beta_warning fields before relying on the screen."
)
