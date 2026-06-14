"""Extract simple annual KPI values from earnings transcripts."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from typing import Any

from fmp.tools.transcripts import get_earnings_transcript


_TTL = timedelta(hours=24)
_CACHE: dict[tuple[str, str, int], tuple[datetime, tuple[float | None, dict, str]]] = {}


def fetch_kpi_from_transcripts(
    ticker: str,
    kpi_key: str,
    kpi_definition: Any,
    fiscal_year: int,
) -> tuple[float | None, dict, str]:
    """Find the first pattern-hint match across Q1-Q4 transcripts."""

    normalized_ticker = ticker.strip().upper()
    normalized_kpi_key = kpi_key.strip()
    key = (normalized_ticker, normalized_kpi_key, int(fiscal_year))
    now = datetime.now(UTC)
    cached = _CACHE.get(key)
    if cached and now - cached[0] < _TTL:
        return cached[1]

    retrieved_at = now.isoformat()
    pattern_hints = _pattern_hints(kpi_definition)
    quarter_payloads: list[dict[str, Any]] = []

    for quarter in (1, 2, 3, 4):
        transcript = get_earnings_transcript(
            symbol=normalized_ticker,
            year=int(fiscal_year),
            quarter=quarter,
            format="full",
        )
        if not isinstance(transcript, dict):
            transcript = {
                "status": "error",
                "error": "transcript wrapper returned non-dict payload",
            }
        quarter_payloads.append({"quarter": quarter, "payload": transcript})
        if transcript.get("status") == "error":
            continue

        text = _joined_transcript_text(transcript)
        match = _first_pattern_match(text, pattern_hints)
        if match is None:
            continue

        value = _parse_numeric(_match_value(match))
        raw_payload = {
            "status": "success",
            "quarter": quarter,
            "kpi_key": normalized_kpi_key,
            "matched_excerpt": _sentence_excerpt(text, match.start(), match.end()),
            "matched_text": match.group(0),
            "transcript": transcript,
        }
        result = (value, raw_payload, retrieved_at)
        _CACHE[key] = (now, result)
        return result

    result = (
        None,
        {
            "status": "not_found",
            "kpi_key": normalized_kpi_key,
            "quarters": quarter_payloads,
        },
        retrieved_at,
    )
    _CACHE[key] = (now, result)
    return result


def _pattern_hints(kpi_definition: Any) -> list[str]:
    extraction = getattr(kpi_definition, "extraction", None)
    if isinstance(kpi_definition, dict):
        extraction = kpi_definition.get("extraction")
    if isinstance(extraction, dict):
        hints = extraction.get("pattern_hints")
    else:
        hints = getattr(extraction, "pattern_hints", None)
    return [str(hint) for hint in (hints or []) if str(hint).strip()]


def _first_pattern_match(text: str, pattern_hints: list[str]) -> re.Match[str] | None:
    for pattern in pattern_hints:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
        if match:
            return match
    return None


def _joined_transcript_text(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for key in ("prepared_remarks", "qa"):
        segments = payload.get(key)
        if isinstance(segments, list):
            for segment in segments:
                if isinstance(segment, dict) and segment.get("text"):
                    parts.append(str(segment["text"]))
                elif isinstance(segment, str):
                    parts.append(segment)
    exchanges = payload.get("qa_exchanges")
    if isinstance(exchanges, list):
        for exchange in exchanges:
            if isinstance(exchange, dict):
                _collect_strings(exchange, parts)
    if not parts:
        _collect_strings(payload, parts)
    return "\n".join(parts)


def _collect_strings(value: Any, parts: list[str]) -> None:
    if isinstance(value, str):
        parts.append(value)
    elif isinstance(value, dict):
        for key, child in value.items():
            if key in {"metadata", "speakers", "speaker_list"}:
                continue
            _collect_strings(child, parts)
    elif isinstance(value, list):
        for child in value:
            _collect_strings(child, parts)


def _match_value(match: re.Match[str]) -> str:
    groupdict = match.groupdict()
    if groupdict.get("value") is not None:
        return groupdict["value"]
    for value in match.groups():
        if value is not None:
            return value
    numeric = re.search(r"-?\d[\d,]*(?:\.\d+)?%?", match.group(0))
    return numeric.group(0) if numeric else match.group(0)


def _parse_numeric(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    text = str(value).strip().replace(",", "")
    if text.endswith("%"):
        text = text[:-1].strip()
    try:
        return float(text)
    except ValueError:
        return None


def _sentence_excerpt(text: str, start: int, end: int) -> str:
    before = max(text.rfind(".", 0, start), text.rfind("?", 0, start), text.rfind("!", 0, start))
    newline_before = text.rfind("\n", 0, start)
    left = max(before, newline_before)
    right_candidates = [
        pos for pos in (text.find(".", end), text.find("?", end), text.find("!", end), text.find("\n", end)) if pos != -1
    ]
    right = min(right_candidates) + 1 if right_candidates else len(text)
    excerpt = text[left + 1 : right].strip()
    return re.sub(r"\s+", " ", excerpt)


__all__ = ["fetch_kpi_from_transcripts"]
