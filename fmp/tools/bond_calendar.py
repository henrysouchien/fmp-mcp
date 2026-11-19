"""SIFMA U.S. Fixed Income (Bond Market) session calendar.

Data-driven calendar backed by official SIFMA holiday and early-close schedule
recommendations. Ingested from published schedule with full provenance, never
hardcoded in code literals.
"""

from __future__ import annotations

import functools
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from ..exceptions import FMPDefinitionArtifactError

_ARTIFACT_PATH = (
    Path(__file__).resolve().parents[1]
    / "definition_artifacts"
    / "sifma_holiday_schedule.json"
)


class _ArtifactModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class _RawArtifact(_ArtifactModel):
    path: str
    sha256: str
    url: str


class _Provenance(_ArtifactModel):
    source_url: str
    source_archive_url: str
    raw_artifacts: list[_RawArtifact]
    publisher: str
    market: str
    market_code: str
    retrieved_at: str
    years_covered: list[int] = Field(min_length=1)
    description: str


class _Holiday(_ArtifactModel):
    date: str
    status: Literal["closed", "early_close"]
    holiday_name: str
    early_close_time: str | None


class _Schedule(_ArtifactModel):
    schema_url: str = Field(alias="$schema")
    data_source: str
    version: str
    provenance: _Provenance
    holidays: list[_Holiday]


@functools.lru_cache(maxsize=1)
def load_sifma_schedule() -> dict[str, Any]:
    """Parse the SIFMA artifact once before exposing it to cached readers."""
    try:
        data = json.loads(_ARTIFACT_PATH.read_text(encoding="utf-8"))
        _Schedule.model_validate(data)
    except (OSError, UnicodeError, ValueError):
        raise FMPDefinitionArtifactError(
            "Invalid SIFMA holiday schedule artifact"
        ) from None
    return data


@functools.lru_cache(maxsize=1)
def _holidays_index() -> dict[str, dict[str, Any]]:
    """Return map of ISO date -> holiday entry."""
    data = load_sifma_schedule()
    return {entry["date"]: entry for entry in data["holidays"]}


def covered_years() -> tuple[int, ...]:
    """Return tuple of years covered by the ingested SIFMA schedule."""
    data = load_sifma_schedule()
    return tuple(data["provenance"]["years_covered"])


def _parse_date(d: date | str) -> date:
    if isinstance(d, date):
        return d
    return date.fromisoformat(d)


def _require_supported_date(d: date) -> None:
    years = covered_years()
    if d.year not in years:
        min_yr, max_yr = min(years), max(years)
        raise ValueError(
            f"Date {d.isoformat()} is outside SIFMA bond calendar coverage ({min_yr}-{max_yr})"
        )


def get_sifma_holiday_entry(d: date | str) -> dict[str, Any] | None:
    """Return the SIFMA holiday recommendation for date if present, else None."""
    parsed = _parse_date(d)
    _require_supported_date(parsed)
    return _holidays_index().get(parsed.isoformat())

def is_bond_market_session(d: date | str) -> bool:
    """Return True if date is an active U.S. bond market trading session.

    A date is a trading session if:
    1. It is a weekday (Monday-Friday)
    2. It is NOT recommended for full market close by SIFMA.
    Early-close days (e.g. 12:00 PM or 2:00 PM ET close) ARE trading sessions.
    """
    parsed = _parse_date(d)
    _require_supported_date(parsed)
    if parsed.weekday() >= 5:
        return False
    entry = get_sifma_holiday_entry(parsed)
    if entry is not None and entry.get("status") == "closed":
        return False
    return True


def last_bond_market_session(
    on_or_before: date | str,
    max_lookback_days: int = 15,
) -> str:
    """Find the most recent bond-market session date on or before the target.

    Returns ISO date string (YYYY-MM-DD).
    """
    current = _parse_date(on_or_before)
    _require_supported_date(current)
    for _ in range(max_lookback_days + 1):
        if is_bond_market_session(current):
            return current.isoformat()
        current -= timedelta(days=1)
    raise ValueError(
        f"No bond market session found within {max_lookback_days} days on or before {on_or_before}"
    )


def get_bond_market_calendar(
    from_date: date | str,
    to_date: date | str,
    *,
    sessions_only: bool = False,
) -> list[dict[str, Any]]:
    """Enumerate bond market calendar days or sessions in [from_date, to_date].

    If sessions_only=True, returns only active sessions (matching SPY EOD record dates).
    """
    start = _parse_date(from_date)
    end = _parse_date(to_date)
    _require_supported_date(start)
    _require_supported_date(end)
    if start > end:
        raise ValueError(f"from_date ({from_date}) must be <= to_date ({to_date})")

    index = _holidays_index()
    results: list[dict[str, Any]] = []
    current = start
    while current <= end:
        iso_str = current.isoformat()
        weekday = current.weekday()
        entry = index.get(iso_str)
        if weekday >= 5:
            is_session = False
            status = "weekend"
            early_close = None
            holiday_name = None
        elif entry is not None and entry.get("status") == "closed":
            is_session = False
            status = "closed"
            early_close = None
            holiday_name = entry.get("holiday_name")
        elif entry is not None and entry.get("status") == "early_close":
            is_session = True
            status = "early_close"
            early_close = entry.get("early_close_time")
            holiday_name = entry.get("holiday_name")
        else:
            is_session = True
            status = "open"
            early_close = None
            holiday_name = None

        record = {
            "date": iso_str,
            "market": "US_BOND",
            "is_session": is_session,
            "status": status,
            "early_close_time": early_close,
            "holiday_name": holiday_name,
        }
        if not sessions_only or is_session:
            results.append(record)
        current += timedelta(days=1)

    return results
