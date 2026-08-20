"""Field-scoped semantic definitions captured at the FMP provider boundary."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Literal, Mapping

from .exceptions import FMPDefinitionArtifactError


FMPFieldSemantic = Literal[
    "non_gaap",
    "target_period_end",
]
FMPFieldCurrencySemantics = Literal["issuer_reporting_currency"]
FMPFieldNumericScale = Literal["currency_units"]
FMPDefinitionUnavailableReason = Literal[
    "field_not_documented",
    "definition_not_available_at_observation",
]

_ARTIFACT_PATH = (
    Path(__file__).with_name("definition_artifacts")
    / "analyst_estimates_faq_2026-08-18.json"
)
_ARTIFACT_SHA256 = (
    "3ed02313012ab0e01ba410212d112ae4fd50e58a982ae434d2eb74eb7f436d92"
)
_SCHEMA_VERSION = "fmp.provider-definition-capture.v1"
_SOURCE_URL = "https://site.financialmodelingprep.com/contact"
_SOURCE_KIND = "normalized_bounded_official_faq_capture"
_EXPECTED_CLAIMS = {
    ("analyst_estimates", "epsAvg"): (
        "non_gaap",
        "Are the analyst estimates for Net Income and EPS in your API GAAP or non-GAAP,",
        "The Analyst Estimates endpoint returns non-GAAP projection data (including EPS).",
    ),
    ("analyst_estimates", "ebitdaAvg"): (
        "non_gaap",
        "Are the analyst estimates for Net Income and EPS in your API GAAP or non-GAAP,",
        "The Analyst Estimates endpoint returns non-GAAP projection data (including EPS).",
    ),
    ("analyst_estimates", "date"): (
        "target_period_end",
        "Why do analyst estimate dates differ from earnings surprise dates?",
        "Analyst Estimates API uses the end-of-period date for the estimate.",
    ),
}


@dataclass(frozen=True, slots=True)
class FMPFieldDefinitionAvailable:
    """A definition supported by a field-scoped, time-valid FMP capture."""

    endpoint: str
    native_field: str
    semantic: FMPFieldSemantic
    response_observed_at: datetime
    provider: Literal["fmp"]
    source_url: str
    source_kind: Literal["normalized_bounded_official_faq_capture"]
    faq_heading: str
    evidence_text: str
    captured_at: datetime
    artifact_sha256: str
    currency_semantics: FMPFieldCurrencySemantics | None = None
    numeric_scale: FMPFieldNumericScale | None = None
    status: Literal["available"] = "available"


@dataclass(frozen=True, slots=True)
class FMPFieldDefinitionUnavailable:
    """A closed result when no time-valid field definition is available."""

    endpoint: str
    native_field: str
    response_observed_at: datetime
    reason: FMPDefinitionUnavailableReason
    definition_captured_at: datetime
    status: Literal["unavailable"] = "unavailable"


FMPFieldDefinition = (
    FMPFieldDefinitionAvailable | FMPFieldDefinitionUnavailable
)


@dataclass(frozen=True, slots=True)
class _DefinitionClaim:
    endpoint: str
    native_field: str
    semantic: FMPFieldSemantic
    faq_heading: str
    evidence_text: str


@dataclass(frozen=True, slots=True)
class _DefinitionArtifact:
    captured_at: datetime
    artifact_sha256: str
    claims: tuple[_DefinitionClaim, ...]


def get_fmp_field_definition(
    endpoint: str,
    native_field: str,
    *,
    response_observed_at: datetime,
) -> FMPFieldDefinition:
    """Return a field definition only when it existed by response observation."""

    observed_at = _require_utc(
        response_observed_at,
        field_name="response_observed_at",
    )
    artifact = _load_definition_artifact()
    claim = next(
        (
            candidate
            for candidate in artifact.claims
            if candidate.endpoint == endpoint
            and candidate.native_field == native_field
        ),
        None,
    )
    if claim is None:
        return FMPFieldDefinitionUnavailable(
            endpoint=endpoint,
            native_field=native_field,
            response_observed_at=observed_at,
            reason="field_not_documented",
            definition_captured_at=artifact.captured_at,
        )
    if observed_at < artifact.captured_at:
        return FMPFieldDefinitionUnavailable(
            endpoint=endpoint,
            native_field=native_field,
            response_observed_at=observed_at,
            reason="definition_not_available_at_observation",
            definition_captured_at=artifact.captured_at,
        )
    return FMPFieldDefinitionAvailable(
        endpoint=claim.endpoint,
        native_field=claim.native_field,
        semantic=claim.semantic,
        response_observed_at=observed_at,
        provider="fmp",
        source_url=_SOURCE_URL,
        source_kind=_SOURCE_KIND,
        faq_heading=claim.faq_heading,
        evidence_text=claim.evidence_text,
        captured_at=artifact.captured_at,
        artifact_sha256=artifact.artifact_sha256,
    )


def _load_definition_artifact() -> _DefinitionArtifact:
    try:
        raw = _ARTIFACT_PATH.read_bytes()
    except OSError as exc:
        raise FMPDefinitionArtifactError(
            "FMP definition artifact is unavailable"
        ) from exc
    actual_sha256 = hashlib.sha256(raw).hexdigest()
    if actual_sha256 != _ARTIFACT_SHA256:
        raise FMPDefinitionArtifactError(
            "FMP definition artifact SHA-256 mismatch"
        )
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FMPDefinitionArtifactError(
            "FMP definition artifact is not valid JSON"
        ) from exc
    if _canonical_json_bytes(payload) != raw:
        raise FMPDefinitionArtifactError(
            "FMP definition artifact is not canonical JSON"
        )
    return _parse_definition_artifact(payload, actual_sha256)


def _parse_definition_artifact(
    payload: object,
    artifact_sha256: str,
) -> _DefinitionArtifact:
    root = _require_mapping(payload, "artifact")
    _require_exact_keys(
        root,
        {
            "captured_at",
            "claims",
            "provider",
            "schema_version",
            "source_kind",
            "source_url",
        },
        "artifact",
    )
    if root["schema_version"] != _SCHEMA_VERSION:
        raise FMPDefinitionArtifactError(
            "FMP definition artifact schema version drifted"
        )
    if root["provider"] != "fmp":
        raise FMPDefinitionArtifactError(
            "FMP definition artifact provider drifted"
        )
    if root["source_url"] != _SOURCE_URL or root["source_kind"] != _SOURCE_KIND:
        raise FMPDefinitionArtifactError(
            "FMP definition artifact source identity drifted"
        )
    captured_at = _parse_utc_timestamp(root["captured_at"], "captured_at")
    raw_claims = root["claims"]
    if not isinstance(raw_claims, list):
        raise FMPDefinitionArtifactError(
            "FMP definition artifact claims must be a list"
        )

    claims = tuple(_parse_claim(claim) for claim in raw_claims)
    actual_claims = {
        (claim.endpoint, claim.native_field): (
            claim.semantic,
            claim.faq_heading,
            claim.evidence_text,
        )
        for claim in claims
    }
    if len(actual_claims) != len(claims) or actual_claims != _EXPECTED_CLAIMS:
        raise FMPDefinitionArtifactError(
            "FMP definition artifact field scope or locator drifted"
        )
    return _DefinitionArtifact(
        captured_at=captured_at,
        artifact_sha256=artifact_sha256,
        claims=claims,
    )


def _parse_claim(payload: object) -> _DefinitionClaim:
    claim = _require_mapping(payload, "claim")
    _require_exact_keys(
        claim,
        {
            "endpoint",
            "evidence_text",
            "locator",
            "native_field",
            "semantic",
        },
        "claim",
    )
    locator = _require_mapping(claim["locator"], "claim locator")
    _require_exact_keys(locator, {"faq_heading", "kind"}, "claim locator")
    if locator["kind"] != "faq_heading":
        raise FMPDefinitionArtifactError(
            "FMP definition artifact locator kind drifted"
        )
    values = {
        key: claim[key]
        for key in ("endpoint", "native_field", "semantic", "evidence_text")
    }
    values["faq_heading"] = locator["faq_heading"]
    if not all(isinstance(value, str) and value for value in values.values()):
        raise FMPDefinitionArtifactError(
            "FMP definition artifact claim values must be non-empty strings"
        )
    if values["semantic"] not in ("non_gaap", "target_period_end"):
        raise FMPDefinitionArtifactError(
            "FMP definition artifact semantic is unsupported"
        )
    return _DefinitionClaim(
        endpoint=values["endpoint"],
        native_field=values["native_field"],
        semantic=values["semantic"],
        faq_heading=values["faq_heading"],
        evidence_text=values["evidence_text"],
    )


def _require_mapping(value: object, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise FMPDefinitionArtifactError(
            f"FMP definition {field_name} must be an object"
        )
    return value


def _require_exact_keys(
    value: Mapping[str, Any],
    expected: set[str],
    field_name: str,
) -> None:
    if set(value) != expected:
        raise FMPDefinitionArtifactError(
            f"FMP definition {field_name} schema drifted"
        )


def _parse_utc_timestamp(value: object, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise FMPDefinitionArtifactError(
            f"FMP definition {field_name} must be an ISO-8601 timestamp"
        )
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise FMPDefinitionArtifactError(
            f"FMP definition {field_name} must be an ISO-8601 timestamp"
        ) from exc
    try:
        return _require_utc(parsed, field_name=field_name)
    except ValueError as exc:
        raise FMPDefinitionArtifactError(str(exc)) from exc


def _require_utc(value: datetime, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    offset = value.utcoffset() if value.tzinfo is not None else None
    if offset != timedelta(0):
        raise ValueError(f"{field_name} must be an aware UTC datetime")
    return value.astimezone(timezone.utc)


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        + "\n"
    ).encode("utf-8")


__all__ = [
    "FMPDefinitionUnavailableReason",
    "FMPFieldDefinition",
    "FMPFieldDefinitionAvailable",
    "FMPFieldDefinitionUnavailable",
    "FMPFieldSemantic",
    "get_fmp_field_definition",
]
