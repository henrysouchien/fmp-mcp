"""Deterministic, provider-owned lineage descriptors for FMP responses."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "fms.vendor-response.v1"


def build_vendor_response_descriptor(
    endpoint: str,
    params: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Describe the exact filtered response and its scalar result coordinates.

    This descriptor is created beside the provider response. Consumers may link
    claims to issued result keys, but must not reconstruct provider coordinates.
    """

    request = {"endpoint": str(endpoint), "params": _json_value(dict(params))}
    response = [_json_value(dict(record)) for record in records]
    request_sha256 = _sha256(request)
    response_sha256 = _sha256(response)
    values: list[dict[str, Any]] = []
    for row_index, record in enumerate(response):
        if not isinstance(record, dict):
            continue
        period = _period(record)
        for field in sorted(record):
            value = record[field]
            canonical = _canonical_scalar(value)
            if canonical is None:
                continue
            value_kind, canonical_value = canonical
            entry: dict[str, Any] = {
                "result_key": f"data[{row_index}].{field}",
                "row_index": row_index,
                "field": field,
                "value_kind": value_kind,
                "canonical_value": canonical_value,
                "concept": field,
            }
            if period is not None and field != "date":
                entry["period"] = period
            values.append(entry)
    return {
        "schema_version": SCHEMA_VERSION,
        "issuer": "risk_module",
        "provider": "fmp",
        "endpoint": str(endpoint),
        "request_sha256": request_sha256,
        "response_sha256": response_sha256,
        "values": values,
    }


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("FMP lineage values must be finite")
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("FMP lineage values must be finite")
        return format(value, "f")
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(child) for key, child in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_json_value(child) for child in value]
    if hasattr(value, "item"):
        return _json_value(value.item())
    raise TypeError(f"unsupported FMP lineage value: {type(value).__name__}")


def _canonical_scalar(value: Any) -> tuple[str, str] | None:
    if type(value) is bool or value is None:
        return None
    if type(value) is int:
        return "integer", str(value)
    if isinstance(value, (float, Decimal)):
        try:
            parsed = Decimal(str(value))
        except InvalidOperation as exc:
            raise ValueError("FMP lineage numeric value is invalid") from exc
        if not parsed.is_finite():
            raise ValueError("FMP lineage numeric value must be finite")
        text = format(parsed, "f")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return "decimal", "0" if text == "-0" else text
    if isinstance(value, str) and value.strip():
        text = value.strip()
        if _looks_like_date(text):
            return "date", text
    return None


def _period(record: Mapping[str, Any]) -> str | None:
    for key in ("date", "period", "calendarYear", "fiscalYear"):
        value = record.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def _looks_like_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return True


def _sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


__all__ = ["SCHEMA_VERSION", "build_vendor_response_descriptor"]
