"""
MCP Tool: get_earnings_transcript

Parses earnings call transcripts into navigable, filterable chunks.
Raw transcripts are 15-65KB of unstructured text. This tool splits them
into prepared remarks, Q&A segments, and individual Q&A exchanges so
an agent can scout, select, and read only what it needs.

Follows the agent-tool response protocol (see PROTOCOL_agent_tool_responses.md):
- Default format="summary" returns metadata only (speaker list, word counts,
  exchange count). No text content. Costs ~1 KB of context.
- format="full" returns text content, with each text field truncated to
  max_words (default 3000) to protect the agent's context window.
- Truncated fields include a continuation marker:
  "...[truncated — N more words remaining]"

Architecture note:
- Fetches raw transcript via FMPClient.fetch() (returns single-row DataFrame)
- Parses into speaker segments, classifies roles, detects Q&A boundary
- Caches parsed result as JSON in cache/transcripts_parsed/
- Registered on fmp-mcp server
- stdout is redirected to stderr to protect MCP JSON-RPC channel from stray prints
"""

import copy
import hashlib
import html
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Literal, Optional

from ..client import FMPClient
from ..exceptions import FMPEmptyResponseError
from ._file_output import _cache_base, atomic_write_text

# Parser version for cache invalidation. Bump when parsing logic changes.
# Included in cache filename so old caches are naturally bypassed.
PARSER_VERSION = 1

# False positive speaker names to ignore (these appear as "Note:", "Source:", etc.)
FALSE_POSITIVE_SPEAKERS = frozenset({
    "Note",
    "Source",
    "Disclaimer",
    "Company",
    "Forward",
    "Safe",
    "Harbor",
    "Important",
    "Copyright",
})

# Speaker regex: captures "Name:" at start of line
# FMP transcripts use "Name: text..." format (one line per speaker)
SPEAKER_PATTERN = re.compile(
    r"^([A-Z][a-zA-Z.'\-]+(?:\s+[A-Z][a-zA-Z.'\-]+){0,4})\s*:"
)

# Patterns to detect Q&A boundary in speaker text
QA_BOUNDARY_PATTERNS = [
    re.compile(r"open.*(?:call|line).*(?:question|Q&A)", re.IGNORECASE),
    re.compile(r"(?:first|begin|start)\s+(?:the\s+)?question", re.IGNORECASE),
    re.compile(r"Q\s*&\s*A\s+(?:session|portion|segment)", re.IGNORECASE),
    re.compile(r"open\s+(?:it\s+)?up\s+(?:for|to)\s+question", re.IGNORECASE),
    re.compile(r"take\s+(?:our\s+)?(?:first\s+)?question", re.IGNORECASE),
]

# Role detection patterns (applied to intro text to build name->role mapping)
ROLE_PATTERNS = {
    "CEO": re.compile(
        r"\bCEO\b|Chief\s+Executive\s+Officer|"
        r"(?<!\bVice\s)(?<!\bSenior\s)\bPresident\b(?!\s+of\s+(?:Investor|IR|Finance|Financial))",
        re.IGNORECASE,
    ),
    "CFO": re.compile(r"\bCFO\b|Chief\s+Financial\s+Officer", re.IGNORECASE),
    "COO": re.compile(r"\bCOO\b|Chief\s+Operating\s+Officer", re.IGNORECASE),
    "CTO": re.compile(r"\bCTO\b|Chief\s+Technology\s+Officer", re.IGNORECASE),
    "IR": re.compile(
        r"Investor\s+Relations|Director.*Investor|Head\s+of\s+Investor",
        re.IGNORECASE,
    ),
}

# Analyst introduction patterns in Operator text
# Multiple patterns tried in order (first match wins). Covers observed Operator phrasings:
#   "question from Name with Firm"
#   "question comes from Name with Firm"
#   "is from Name with Firm"
#   "line of Name with Firm"
#   "will come from Name with Firm"
#   "Name from Firm" (simpler fallback)
ANALYST_INTRO_PATTERNS = [
    re.compile(
        r"(?:question\s+(?:from|comes\s+from|is\s+from)|turn.*over\s+to)\s+"
        r"([A-Z][a-zA-Z.'\-]+(?:\s+[A-Z][a-zA-Z.'\-]+){0,3})"
        r"\s+(?:with|from|of|at)\s+(.+?)(?:\.|Please|$)",
        re.IGNORECASE,
    ),
    re.compile(
        r"line\s+of\s+"
        r"([A-Z][a-zA-Z.'\-]+(?:\s+[A-Z][a-zA-Z.'\-]+){0,3})"
        r"\s+(?:with|from|of|at)\s+(.+?)(?:\.|Please|$)",
        re.IGNORECASE,
    ),
    re.compile(
        r"will\s+come\s+from\s+"
        r"([A-Z][a-zA-Z.'\-]+(?:\s+[A-Z][a-zA-Z.'\-]+){0,3})"
        r"\s+(?:with|from|of|at)\s+(.+?)(?:\.|Please|$)",
        re.IGNORECASE,
    ),
    re.compile(
        r"([A-Z][a-zA-Z.'\-]+(?:\s+[A-Z][a-zA-Z.'\-]+){0,3})"
        r"\s+(?:from|with)\s+(.+?)(?:\.|Please|Your|$)",
        re.IGNORECASE,
    ),
]

_CACHE_BASE = _cache_base()
PARSED_CACHE_DIR = _CACHE_BASE / "cache" / "transcripts_parsed"
FILE_OUTPUT_DIR = _CACHE_BASE / "cache" / "file_output"
MAX_BASENAME = 180


def _normalize_content(content: str) -> str:
    """
    Normalize raw transcript text before speaker parsing.

    Handles edge cases from the spec that can appear in FMP data:
    - Markdown bold speaker names: **Speaker**: -> Speaker:
    - Bracketed speaker names: [Speaker]: -> Speaker:
    - Unicode dashes (em-dash, en-dash) -> ASCII hyphen
    - Unicode quotes (curly single/double) -> ASCII quotes
    - Common HTML entities: &amp; -> &, &quot; -> ", &#39; -> ', etc.
    """
    content = html.unescape(content)
    content = re.sub(r"^\*\*(.+?)\*\*\s*:", r"\1:", content, flags=re.MULTILINE)
    content = re.sub(r"^\[(.+?)\]\s*:", r"\1:", content, flags=re.MULTILINE)
    content = content.replace("\u2013", "-").replace("\u2014", "-")
    content = content.replace("\u2018", "'").replace("\u2019", "'")
    content = content.replace("\u201c", '"').replace("\u201d", '"')
    return content


def parse_speakers(content: str) -> list[dict]:
    """
    Split raw transcript text into speaker segments.

    Args:
        content: Raw transcript text from FMP (one speaker per line).
                 Should be pre-normalized via _normalize_content().

    Returns:
        List of dicts: [{"speaker": str, "text": str, "line_index": int}, ...]
        Each dict represents one speaker turn.
    """
    segments: list[dict] = []
    current: dict | None = None

    for line_index, raw_line in enumerate(content.splitlines()):
        line = raw_line.strip()
        if not line:
            continue

        match = SPEAKER_PATTERN.match(line)
        if match:
            speaker = match.group(1).strip()
            if speaker in FALSE_POSITIVE_SPEAKERS:
                # Treat false-positive speaker lines as continuation text.
                if current is not None:
                    current["text"] = (current["text"] + " " + line).strip()
                continue

            if current is not None:
                segments.append(current)

            text = line[match.end():].strip()
            current = {
                "speaker": speaker,
                "text": text,
                "line_index": line_index,
            }
            continue

        # Defensive fallback for unexpected multiline transcript formats.
        if current is not None:
            current["text"] = (current["text"] + " " + line).strip()

    if current is not None:
        segments.append(current)

    return segments


def find_qa_boundary(segments: list[dict]) -> int | None:
    """
    Find the segment index where Q&A begins.

    Strategy (in priority order):
    1. Explicit text markers in literal "Operator" segments ONLY.
    2. Literal "Operator" segment containing "question" with guards.
    3. Fallback for no-Operator transcripts:
       first NEW speaker after >=3 unique non-Operator speakers, where i-1 has
       explicit Q&A transition language.

    Returns:
        Index into segments list where Q&A starts, or None if no Q&A found.
    """

    def _is_operator(speaker: str) -> bool:
        return speaker.strip().lower() == "operator"

    # Strategy 1: explicit boundary markers in literal Operator segments.
    for i, segment in enumerate(segments):
        if not _is_operator(segment.get("speaker", "")):
            continue
        text = segment.get("text", "")
        if any(pattern.search(text) for pattern in QA_BOUNDARY_PATTERNS):
            return min(i + 1, len(segments))

    # Strategy 2: Operator mentioning "question", with guards.
    for i, segment in enumerate(segments):
        if i < 3 or not _is_operator(segment.get("speaker", "")):
            continue
        text = segment.get("text", "")
        if not re.search(r"\bquestion\b", text, re.IGNORECASE):
            continue
        prior_non_operator = {
            s.get("speaker", "")
            for s in segments[:i]
            if not _is_operator(s.get("speaker", ""))
        }
        if len(prior_non_operator) >= 2:
            return i

    # Strategy 3: no-Operator fallback with tight cue requirement in i-1.
    cue_pattern = re.compile(
        r"first\s+question|next\s+question|ask\s+a\s+question|go\s+ahead|line\s+is\s+open",
        re.IGNORECASE,
    )
    seen_non_operator: set[str] = set()
    for i, segment in enumerate(segments):
        speaker = segment.get("speaker", "").strip()
        if not speaker or _is_operator(speaker):
            continue

        is_new_speaker = speaker not in seen_non_operator
        if i > 0 and is_new_speaker and len(seen_non_operator) >= 3:
            prev_text = segments[i - 1].get("text", "")
            if cue_pattern.search(prev_text):
                return i

        seen_non_operator.add(speaker)

    return None


def _names_match(name_a: str, name_b: str) -> bool:
    """
    Fuzzy name match. Returns True if last names match and first names
    share a 3+ character prefix. Handles "Tim Cook" vs "Timothy Cook".
    """

    def _parts(name: str) -> list[str]:
        return [p for p in re.sub(r"[^a-zA-Z\s]", "", name).lower().split() if p]

    if name_a.strip().lower() == name_b.strip().lower():
        return True

    a_parts = _parts(name_a)
    b_parts = _parts(name_b)
    if not a_parts or not b_parts:
        return False

    if a_parts[-1] != b_parts[-1]:
        return False

    first_a = a_parts[0]
    first_b = b_parts[0]
    if len(first_a) < 3 or len(first_b) < 3:
        return first_a == first_b

    return first_a.startswith(first_b[:3]) or first_b.startswith(first_a[:3])


def classify_roles(
    segments: list[dict],
    qa_boundary: int | None,
) -> None:
    """
    Classify each speaker's role. Mutates segments in place, adding
    'role' and 'firm' keys to each segment dict.

    Strategy:
    1. "Operator" name -> role="Operator"
    2. Parse early/pre-boundary segments for management role mappings
    3. Parse Operator text for analyst introductions (name + firm)
    4. Apply known mappings to all segments
    5. After Q&A boundary: remaining unknown speakers -> Analyst, unless they
       spoke in prepared remarks (known management guard)
    6. Before Q&A boundary: remaining unknown speakers -> Other
    """

    def _is_operator(speaker: str) -> bool:
        return speaker.strip().lower() == "operator"

    def _canonical_role(role_text: str) -> str | None:
        for role, pattern in ROLE_PATTERNS.items():
            if pattern.search(role_text):
                return role
        return None

    for segment in segments:
        if _is_operator(segment.get("speaker", "")):
            segment["role"] = "Operator"
            segment["firm"] = ""

    # Role scanning is intentionally limited to pre-boundary segments only.
    scan_limit = qa_boundary if qa_boundary is not None else max(1, (len(segments) + 1) // 2)
    intro_segments = segments[:scan_limit]

    name_to_role: dict[str, str] = {}
    role_then_name = re.compile(
        r"(?P<role>CEO|CFO|COO|CTO|"
        r"Chief\s+Executive\s+Officer|Chief\s+Financial\s+Officer|"
        r"Chief\s+Operating\s+Officer|Chief\s+Technology\s+Officer|"
        r"Director\s+of\s+Investor\s+Relations|Investor\s+Relations)"
        r"(?:\s+and\s+Co-Founder)?[,\s]+"
        r"(?P<name>[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})",
        re.IGNORECASE,
    )
    name_then_role = re.compile(
        r"(?P<name>[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})[,\s]+"
        r"(?:our\s+)?(?:the\s+)?"
        r"(?P<role>CEO|CFO|COO|CTO|"
        r"Chief\s+Executive\s+Officer|Chief\s+Financial\s+Officer|"
        r"Chief\s+Operating\s+Officer|Chief\s+Technology\s+Officer)",
        re.IGNORECASE,
    )

    # Collect all known speaker names for anchored lookup in intro text.
    all_speaker_names = {
        s["speaker"] for s in segments
        if not _is_operator(s.get("speaker", ""))
    }

    for segment in intro_segments:
        speaker = segment.get("speaker", "")
        text = segment.get("text", "")

        if ROLE_PATTERNS["IR"].search(text):
            name_to_role[speaker] = "IR"

        for match in role_then_name.finditer(text):
            role = _canonical_role(match.group("role"))
            name = match.group("name").strip()
            if role and name:
                name_to_role.setdefault(name, role)

        for match in name_then_role.finditer(text):
            role = _canonical_role(match.group("role"))
            name = match.group("name").strip()
            if role and name:
                name_to_role.setdefault(name, role)

        # Speaker-anchored scan: for each known speaker name that appears
        # in intro text, check if a role keyword follows their name. This
        # handles formats like "Jensen Huang, President and Chief Executive
        # Officer" where the generic name_then_role regex captures title
        # words ("President and") as the name instead of the real name.
        for spk_name in all_speaker_names:
            if spk_name in name_to_role:
                continue
            idx = text.find(spk_name)
            if idx < 0:
                continue
            after = text[idx + len(spk_name):idx + len(spk_name) + 200]
            role = _canonical_role(after)
            if role:
                name_to_role[spk_name] = role

    analyst_firms: dict[str, str] = {}
    for segment in segments:
        if not _is_operator(segment.get("speaker", "")):
            continue
        text = segment.get("text", "")
        for pattern in ANALYST_INTRO_PATTERNS:
            match = pattern.search(text)
            if not match:
                continue
            analyst = match.group(1).strip()
            firm = match.group(2).strip(" .")
            if analyst:
                analyst_firms[analyst] = firm
            break

    for i, segment in enumerate(segments):
        speaker = segment.get("speaker", "")
        if _is_operator(speaker):
            continue

        segment.setdefault("firm", "")

        if "role" not in segment:
            for known_name, role in name_to_role.items():
                if _names_match(speaker, known_name):
                    segment["role"] = role
                    break

        if "role" not in segment:
            for analyst_name, analyst_firm in analyst_firms.items():
                if _names_match(speaker, analyst_name):
                    segment["role"] = "Analyst"
                    segment["firm"] = analyst_firm
                    break

        if "role" not in segment and i < scan_limit and ROLE_PATTERNS["IR"].search(segment.get("text", "")):
            segment["role"] = "IR"

    known_management_speakers: set[str] = set()
    if qa_boundary is not None:
        known_management_speakers = {
            s.get("speaker", "")
            for s in segments[:qa_boundary]
            if not _is_operator(s.get("speaker", ""))
        }

        for i, segment in enumerate(segments):
            if i < qa_boundary:
                continue
            if _is_operator(segment.get("speaker", "")) or "role" in segment:
                continue
            if segment.get("speaker", "") in known_management_speakers:
                segment["role"] = "Other"
                segment["firm"] = ""
            else:
                segment["role"] = "Analyst"
                segment.setdefault("firm", "")

    for segment in segments:
        if "role" not in segment:
            segment["role"] = "Other"
            segment["firm"] = ""


def build_qa_exchanges(qa_segments: list[dict]) -> list[dict]:
    """
    Group Q&A segments into analyst question + management answer exchanges.

    Each exchange: one analyst asks, one or more management respond.
    Operator segments act as exchange separators (not included in exchanges).
    """
    exchanges: list[dict] = []
    current_exchange: dict | None = None

    for segment in qa_segments:
        role = segment.get("role", "")
        speaker = segment.get("speaker", "")
        text = segment.get("text", "").strip()

        if role == "Operator":
            if current_exchange is not None and (current_exchange["question"] or current_exchange["answers"]):
                exchanges.append(current_exchange)
            current_exchange = None
            continue

        if role == "Analyst":
            if current_exchange is None:
                current_exchange = {
                    "analyst": speaker,
                    "firm": segment.get("firm", ""),
                    "question": text,
                    "answers": [],
                }
            elif current_exchange["answers"]:
                exchanges.append(current_exchange)
                current_exchange = {
                    "analyst": speaker,
                    "firm": segment.get("firm", ""),
                    "question": text,
                    "answers": [],
                }
            elif _names_match(current_exchange["analyst"], speaker):
                current_exchange["question"] = (current_exchange["question"] + " " + text).strip()
                if segment.get("firm") and not current_exchange.get("firm"):
                    current_exchange["firm"] = segment.get("firm", "")
            else:
                exchanges.append(current_exchange)
                current_exchange = {
                    "analyst": speaker,
                    "firm": segment.get("firm", ""),
                    "question": text,
                    "answers": [],
                }
            continue

        if current_exchange is not None:
            current_exchange["answers"].append({
                "speaker": speaker,
                "role": role,
                "text": text,
            })

    if current_exchange is not None and (current_exchange["question"] or current_exchange["answers"]):
        exchanges.append(current_exchange)

    return exchanges


def parse_transcript(content: str) -> dict:
    """
    Main parser. Splits transcript into structured sections.

    Args:
        content: Raw transcript text from FMP.

    Returns:
        Dict with keys: prepared_remarks, qa, qa_exchanges, metadata.
    """
    content = _normalize_content(content)
    segments = parse_speakers(content)
    qa_boundary = find_qa_boundary(segments)
    classify_roles(segments, qa_boundary)

    if qa_boundary is None:
        prepared_segments = segments
        qa_segments: list[dict] = []
    else:
        prepared_segments = segments[:qa_boundary]
        qa_segments = segments[qa_boundary:]

    qa_exchanges = build_qa_exchanges(qa_segments)

    def _format_segment(segment: dict) -> dict:
        text = segment.get("text", "").strip()
        return {
            "speaker": segment.get("speaker", ""),
            "role": segment.get("role", "Other"),
            "text": text,
            "word_count": len(text.split()),
        }

    prepared_remarks = [_format_segment(s) for s in prepared_segments]
    qa = [_format_segment(s) for s in qa_segments]

    prepared_words = sum(s["word_count"] for s in prepared_remarks)
    qa_words = sum(s["word_count"] for s in qa)
    total_words = prepared_words + qa_words

    speaker_totals: dict[str, dict] = {}
    for segment in prepared_remarks + qa:
        name = segment["speaker"]
        if name not in speaker_totals:
            speaker_totals[name] = {
                "name": name,
                "role": segment["role"],
                "word_count": 0,
            }
        speaker_totals[name]["word_count"] += segment["word_count"]
        if speaker_totals[name]["role"] == "Other" and segment["role"] != "Other":
            speaker_totals[name]["role"] = segment["role"]

    speaker_list = sorted(
        speaker_totals.values(),
        key=lambda x: x["word_count"],
        reverse=True,
    )

    non_operator_speakers = {
        s["speaker"]
        for s in prepared_remarks + qa
        if s["speaker"].strip().lower() != "operator"
    }

    return {
        "prepared_remarks": prepared_remarks,
        "qa": qa,
        "qa_exchanges": qa_exchanges,
        "metadata": {
            "total_word_count": total_words,
            "prepared_remarks_word_count": prepared_words,
            "qa_word_count": qa_words,
            "speaker_list": speaker_list,
            "num_qa_exchanges": len(qa_exchanges),
            "num_speakers": len(non_operator_speakers),
        },
    }


def _truncate(text: str, max_words: int | None) -> str:
    """
    Truncate text to max_words, appending continuation marker.
    """
    if max_words is None:
        return text
    words = text.split()
    if len(words) <= max_words:
        return text
    remaining = len(words) - max_words
    return " ".join(words[:max_words]) + f"\n\n...[truncated — {remaining:,} more words remaining]"


def _slugify_component(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9_-]+", "_", value or "")
    slug = re.sub(r"_+", "_", slug).strip("_")
    if not slug:
        return "unknown"
    return slug[:64]


def _canonical_hash8(payload: dict) -> str:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha1(serialized.encode("utf-8")).hexdigest()[:8]


def _finalize_basename(readable_basename: str, hash8: str | None, fallback_payload: dict) -> str:
    suffix = f"_{hash8}" if hash8 else ""
    candidate = f"{readable_basename}{suffix}"
    if len(candidate) <= MAX_BASENAME:
        return candidate

    if not hash8:
        hash8 = _canonical_hash8(fallback_payload)
        suffix = f"_{hash8}"

    keep = max(1, MAX_BASENAME - len(suffix))
    truncated = readable_basename[:keep].rstrip("_") or "file"
    return f"{truncated}{suffix}"[:MAX_BASENAME]


def _safe_heading(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _build_transcript_file_path(
    symbol_raw: str,
    symbol: str,
    year: int,
    quarter: int,
    section_raw: str,
    filter_role_raw: str | None,
    filter_speaker_raw: str | None,
) -> Path:
    speaker_filter_active = bool(filter_speaker_raw and filter_speaker_raw.strip())
    role_filter_active = bool(filter_role_raw)
    section_filtered = section_raw != "all"
    is_filtered = section_filtered or role_filter_active or speaker_filter_active

    payload = {
        "tool": "get_earnings_transcript",
        "symbol_raw": symbol_raw,
        "year": year,
        "quarter": quarter,
        "section_raw": section_raw,
        "filter_role_raw": filter_role_raw,
        "filter_speaker_raw": filter_speaker_raw,
    }

    components = [
        _slugify_component(symbol.upper()),
        f"{quarter}Q{year % 100:02d}",
        "transcript",
    ]
    if section_filtered:
        components.append(_slugify_component(section_raw))
    if role_filter_active:
        components.append(_slugify_component(filter_role_raw or ""))
    if speaker_filter_active:
        components.append(_slugify_component((filter_speaker_raw or "").strip()))

    readable = "_".join(components)
    basename = _finalize_basename(
        readable_basename=readable,
        hash8=_canonical_hash8(payload) if is_filtered else None,
        fallback_payload=payload,
    )
    return FILE_OUTPUT_DIR / f"{basename}.md"


def _write_transcript_markdown(result: dict, file_path: Path) -> None:
    metadata = result.get("metadata", {})
    total_words = int(metadata.get("total_word_count", metadata.get("total_words", 0)) or 0)
    speaker_count = int(metadata.get("num_speakers", metadata.get("speaker_count", 0)) or 0)
    exchange_count = int(metadata.get("num_qa_exchanges", metadata.get("exchange_count", 0)) or 0)
    symbol = str(result.get("symbol", "")).upper()
    quarter = result.get("quarter", "")
    year = result.get("year", "")

    prepared_remarks = result.get("prepared_remarks", [])
    qa_segments = result.get("qa", [])
    qa_exchanges = result.get("qa_exchanges", [])
    has_content = bool(prepared_remarks or qa_segments or qa_exchanges)

    lines = [
        f"# {symbol} Earnings Call - Q{quarter} FY{year}",
        f"> Total words: {total_words:,} | Speakers: {speaker_count} | Exchanges: {exchange_count}",
        "---",
    ]

    if not has_content:
        lines.append("No content matched filters.")
        atomic_write_text(file_path, "\n".join(lines).strip() + "\n")
        return

    if prepared_remarks:
        lines.append("## PREPARED REMARKS")
        for segment in prepared_remarks:
            speaker = _safe_heading(segment.get("speaker", "")) or "Unknown"
            role = _safe_heading(segment.get("role", ""))
            label = f"{speaker} ({role})" if role else speaker
            lines.append(f"### SPEAKER: {label}")
            text = (segment.get("text", "") or "").strip()
            if text:
                lines.append(text)
        lines.append("---")

    if qa_exchanges or qa_segments:
        lines.append("## Q&A SESSION")

    if qa_exchanges:
        for idx, exchange in enumerate(qa_exchanges, start=1):
            analyst = _safe_heading(exchange.get("analyst", "")) or "Unknown Analyst"
            firm = _safe_heading(exchange.get("firm", ""))
            analyst_header = f"{analyst} ({firm})" if firm else analyst
            answers = exchange.get("answers", [])
            if answers:
                first_answer = answers[0]
                first_speaker = _safe_heading(first_answer.get("speaker", "")) or "Management"
                first_role = _safe_heading(first_answer.get("role", ""))
                first_answer_header = (
                    f"{first_speaker} ({first_role})" if first_role else first_speaker
                )
            else:
                first_answer_header = "Management"

            lines.append(f"### EXCHANGE {idx}: {analyst_header} -> {first_answer_header}")
            lines.append(f"**Question ({analyst}, Analyst):**")
            question = (exchange.get("question", "") or "").strip()
            if question:
                lines.append(question)

            for answer in answers:
                speaker = _safe_heading(answer.get("speaker", "")) or "Management"
                role = _safe_heading(answer.get("role", ""))
                answer_label = f"{speaker}, {role}" if role else speaker
                lines.append(f"**Answer ({answer_label}):**")
                text = (answer.get("text", "") or "").strip()
                if text:
                    lines.append(text)
    else:
        for segment in qa_segments:
            speaker = _safe_heading(segment.get("speaker", "")) or "Unknown"
            role = _safe_heading(segment.get("role", ""))
            label = f"{speaker} ({role})" if role else speaker
            lines.append(f"### SPEAKER: {label}")
            text = (segment.get("text", "") or "").strip()
            if text:
                lines.append(text)

    atomic_write_text(file_path, "\n".join(lines).strip() + "\n")


def _get_cache_path(symbol: str, year: int, quarter: int) -> Path:
    """
    Build path for parsed transcript JSON cache.

    Format: cache/transcripts_parsed/{SYMBOL}_{Q}Q{YY}_v{VERSION}_transcript_parsed.json
    Example: cache/transcripts_parsed/AAPL_4Q24_v1_transcript_parsed.json
    """
    return PARSED_CACHE_DIR / (
        f"{symbol.upper()}_{quarter}Q{year % 100:02d}_v{PARSER_VERSION}_transcript_parsed.json"
    )


def _apply_filters(
    parsed: dict,
    section: str,
    filter_speaker: str | None,
    filter_role: str | None,
    format: str,
    max_words: int | None,
    output: Literal["inline", "file"] = "inline",
) -> dict:
    """
    Apply section/speaker/role/format filters to a parsed transcript,
    then apply truncation to text fields.
    """
    result = copy.deepcopy(parsed)
    result.setdefault("prepared_remarks", [])
    result.setdefault("qa", [])
    result.setdefault("qa_exchanges", [])

    # Preserve full-transcript metadata before any filtering so callers
    # always see the complete speaker list / word counts even when their
    # filter matches nothing.
    original_metadata = copy.deepcopy(result.get("metadata", {}))

    if section == "prepared_remarks":
        result["qa"] = []
        result["qa_exchanges"] = []
    elif section == "qa":
        result["prepared_remarks"] = []

    speaker_filter_active = bool(filter_speaker and filter_speaker.strip())
    role_filter_active = bool(filter_role)
    speaker_query = (filter_speaker or "").strip().lower()

    if speaker_filter_active:
        result["prepared_remarks"] = [
            s for s in result["prepared_remarks"]
            if speaker_query in s.get("speaker", "").lower()
        ]
        result["qa"] = [
            s for s in result["qa"]
            if speaker_query in s.get("speaker", "").lower()
        ]

        filtered_exchanges = []
        for ex in result["qa_exchanges"]:
            analyst_match = speaker_query in ex.get("analyst", "").lower()
            answer_match = any(
                speaker_query in ans.get("speaker", "").lower()
                for ans in ex.get("answers", [])
            )
            if analyst_match or answer_match:
                filtered_exchanges.append(ex)
        result["qa_exchanges"] = filtered_exchanges

    if role_filter_active:
        result["prepared_remarks"] = [
            s for s in result["prepared_remarks"]
            if s.get("role") == filter_role
        ]
        result["qa"] = [
            s for s in result["qa"]
            if s.get("role") == filter_role
        ]

        filtered_exchanges = []
        if filter_role == "Analyst":
            filtered_exchanges = [ex for ex in result["qa_exchanges"] if ex.get("analyst")]
        else:
            for ex in result["qa_exchanges"]:
                answer_matches = [
                    ans for ans in ex.get("answers", [])
                    if ans.get("role") == filter_role
                ]
                if answer_matches:
                    ex_copy = copy.deepcopy(ex)
                    ex_copy["answers"] = answer_matches
                    filtered_exchanges.append(ex_copy)
        result["qa_exchanges"] = filtered_exchanges

    # Always report full-transcript metadata so empty-filter results don't
    # look like the transcript itself is empty.
    result["metadata"] = original_metadata

    # Add filtered counts when any filter is active so the caller knows
    # how much content matched.
    any_filter = (section != "all") or bool(filter_speaker and filter_speaker.strip()) or bool(filter_role)
    if any_filter:
        filtered_words_prepared = sum(s.get("word_count", 0) for s in result["prepared_remarks"])
        filtered_words_qa = sum(s.get("word_count", 0) for s in result["qa"])
        result["metadata"]["filtered_word_count"] = filtered_words_prepared + filtered_words_qa
        result["metadata"]["filtered_segment_count"] = len(result["prepared_remarks"]) + len(result["qa"])
        result["metadata"]["filtered_exchange_count"] = len(result["qa_exchanges"])

    if output == "file":
        return result

    preview_mode = (
        format == "full"
        and section == "all"
        and not speaker_filter_active
        and not role_filter_active
    )

    if format == "full":
        if preview_mode:
            result["prepared_remarks"] = result["prepared_remarks"][:3]
            result["qa"] = result["qa"][:3]
            result["qa_exchanges"] = result["qa_exchanges"][:2]

            for segment in result["prepared_remarks"]:
                segment["text"] = _truncate(segment.get("text", ""), 500)
            for segment in result["qa"]:
                segment["text"] = _truncate(segment.get("text", ""), 500)
            for exchange in result["qa_exchanges"]:
                exchange["question"] = _truncate(exchange.get("question", ""), 500)
                for answer in exchange.get("answers", []):
                    answer["text"] = _truncate(answer.get("text", ""), 500)

            result["hint"] = (
                "Showing preview (first 3 segments per section, 500 words each). "
                "Use section, filter_role, or filter_speaker to get specific full content."
            )
        else:
            for segment in result["prepared_remarks"]:
                segment["text"] = _truncate(segment.get("text", ""), max_words)
            for segment in result["qa"]:
                segment["text"] = _truncate(segment.get("text", ""), max_words)
            for exchange in result["qa_exchanges"]:
                exchange["question"] = _truncate(exchange.get("question", ""), max_words)
                for answer in exchange.get("answers", []):
                    answer["text"] = _truncate(answer.get("text", ""), max_words)

    if format == "summary":
        result.pop("prepared_remarks", None)
        result.pop("qa", None)
        result.pop("qa_exchanges", None)
        result["hint"] = (
            "Use format='full' with filters (section, filter_role, filter_speaker) "
            "to read specific content."
        )

    return result


def get_earnings_transcript(
    symbol: str,
    year: int,
    quarter: int,
    section: Literal["prepared_remarks", "qa", "all"] = "all",
    filter_speaker: str | None = None,
    filter_role: Literal["CEO", "CFO", "COO", "CTO", "Analyst", "IR", "Operator"] | None = None,
    format: Literal["full", "summary"] = "summary",
    max_words: int | None = 3000,
    output: Literal["inline", "file"] = "inline",
) -> dict:
    """
    Fetch, parse, and filter an earnings call transcript.

    Default format is "summary" (metadata only: speaker list, word counts,
    exchange count). Use format="full" with filters to read text content.
    When format="full", each text field is truncated to max_words (default 3000).
    """
    _saved = sys.stdout
    sys.stdout = sys.stderr
    try:
        symbol_raw = symbol
        section_raw = section
        filter_role_raw = filter_role
        filter_speaker_raw = filter_speaker
        symbol = symbol.strip().upper()
        if output not in ("inline", "file"):
            return {"status": "error", "error": "output must be 'inline' or 'file'"}
        if max_words is not None and max_words < 1:
            return {"status": "error", "error": "max_words must be >= 1 or None"}
        if quarter not in (1, 2, 3, 4):
            return {"status": "error", "error": "quarter must be 1-4"}
        year_max = datetime.now().year + 2
        if year < 2000 or year > year_max:
            return {"status": "error", "error": f"year must be between 2000 and {year_max}"}

        cache_path = _get_cache_path(symbol, year, quarter)
        if cache_path.is_file():
            with open(cache_path) as f:
                parsed = json.load(f)
        else:
            fmp = FMPClient()
            try:
                df = fmp.fetch("earnings_transcript", symbol=symbol, year=year, quarter=quarter)
            except FMPEmptyResponseError:
                return {
                    "status": "error",
                    "error": f"No transcript found for {symbol} Q{quarter} {year}",
                }

            if df.empty:
                return {
                    "status": "error",
                    "error": f"No transcript found for {symbol} Q{quarter} {year}",
                }

            content = str(df["content"].iloc[0])
            if len(content) < 500:
                return {
                    "status": "error",
                    "error": (
                        f"Transcript too short ({len(content)} chars) for "
                        f"{symbol} Q{quarter} {year}. May be incomplete."
                    ),
                }

            parsed = parse_transcript(content)
            parsed["symbol"] = symbol
            parsed["year"] = year
            parsed["quarter"] = quarter
            parsed["date"] = str(df["date"].iloc[0])[:10] if "date" in df.columns else ""

            cache_path.parent.mkdir(parents=True, exist_ok=True)
            with open(cache_path, "w") as f:
                json.dump(parsed, f)

        result = _apply_filters(
            parsed,
            section,
            filter_speaker,
            filter_role,
            format,
            max_words,
            output=output,
        )
        if output == "file":
            attempted_path = _build_transcript_file_path(
                symbol_raw=symbol_raw,
                symbol=symbol,
                year=year,
                quarter=quarter,
                section_raw=section_raw,
                filter_role_raw=filter_role_raw,
                filter_speaker_raw=filter_speaker_raw,
            )
            is_empty = (
                not result.get("prepared_remarks")
                and not result.get("qa")
                and not result.get("qa_exchanges")
            )

            try:
                _write_transcript_markdown(result, attempted_path)
            except OSError as exc:
                return {
                    "status": "error",
                    "output": "file",
                    "error_code": "FILE_WRITE_ERROR",
                    "message": str(exc),
                    "file_path": str(attempted_path.resolve()),
                }

            response = copy.deepcopy(result)
            response.pop("prepared_remarks", None)
            response.pop("qa", None)
            response.pop("qa_exchanges", None)
            response["symbol"] = symbol
            response["year"] = year
            response["quarter"] = quarter
            response["output"] = "file"
            response["file_path"] = str(attempted_path.resolve())
            response["is_empty"] = is_empty
            response["hint"] = "Use Read tool with file_path. Grep '^### SPEAKER:' for anchors."

            metadata = response.get("metadata", {})
            if is_empty:
                metadata = {
                    "total_word_count": 0,
                    "total_words": 0,
                    "speaker_list": [],
                    "num_speakers": 0,
                    "speaker_count": 0,
                    "num_qa_exchanges": 0,
                    "exchange_count": 0,
                }
                response["speakers"] = []
            else:
                metadata["total_words"] = metadata.get("total_word_count", 0)
                metadata["speaker_count"] = metadata.get(
                    "num_speakers",
                    len(metadata.get("speaker_list", [])),
                )
                metadata["exchange_count"] = metadata.get("num_qa_exchanges", 0)
            response["metadata"] = metadata
            response["status"] = "success"
            return response

        result["status"] = "success"
        return result

    except Exception as e:
        return {"status": "error", "error": str(e)}
    finally:
        sys.stdout = _saved
