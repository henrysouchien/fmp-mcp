"""Provider-owned lineage for normalized FMP responses.

One fetched response has one descriptor (``build_vendor_response_descriptor``,
issued by ``FMPClient.fetch_with_lineage``). A tool that answers from several
responses returns one composite descriptor named for the tool
(``returns_vendor_lineage``): every response the provider boundary returns while
the tool runs is recorded into the tool call's ``VendorResponseLog``.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass, field as dataclass_field
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
import functools
import hashlib
import json
import math
import threading
from types import MappingProxyType
from typing import Any, Callable, Iterator, Literal, Mapping, Sequence, TypeVar

import pandas as pd


SCHEMA_VERSION = "fms.vendor-response.v1"
FMPFetchSource = Literal["live", "cache"]


def build_vendor_response_descriptor(
    endpoint: str,
    params: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Describe a normalized provider response and its scalar coordinates."""

    response = _normalized_response(records)
    return _descriptor_from_normalized_response(endpoint, params, response)


def _descriptor_from_normalized_response(
    endpoint: str,
    params: Mapping[str, Any],
    response: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    request = {"endpoint": str(endpoint), "params": _json_value(dict(params))}
    return _descriptor(
        endpoint,
        request_sha256=_sha256(request),
        response_sha256=_sha256(response),
        values=_scalar_values(response, key_prefix=""),
    )


def _scalar_values(
    response: Sequence[Mapping[str, Any]],
    *,
    key_prefix: str,
) -> list[dict[str, Any]]:
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
                "result_key": f"{key_prefix}data[{row_index}].{field}",
                "row_index": row_index,
                "field": field,
                "value_kind": value_kind,
                "canonical_value": canonical_value,
                "concept": field,
            }
            if period is not None and field != "date":
                entry["period"] = period
            values.append(entry)
    return values


def _descriptor(
    endpoint: str,
    *,
    request_sha256: str,
    response_sha256: str,
    values: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "issuer": "risk_module",
        "provider": "fmp",
        "endpoint": str(endpoint),
        "request_sha256": request_sha256,
        "response_sha256": response_sha256,
        "values": values,
    }


@dataclass(frozen=True, slots=True)
class VendorResponse:
    """One provider request and its JSON-normalized response records."""

    endpoint: str
    params: Mapping[str, Any]
    records: tuple[dict[str, Any], ...]

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(
            {
                "request": {"endpoint": self.endpoint, "params": self.params},
                "response": list(self.records),
            }
        )


def build_composite_vendor_response_descriptor(
    endpoint: str,
    responses: Sequence[VendorResponse],
) -> dict[str, Any]:
    """Describe every provider response one tool call answered from.

    ``endpoint`` is the tool's name, which is the endpoint Hank issues the
    tool's excerpt handle under. Responses are identified by content, so the
    descriptor does not depend on the order concurrent fetches completed in,
    and a response fetched twice is described once.
    """

    unique = {response.canonical_bytes(): response for response in responses}
    ordered = [unique[key] for key in sorted(unique)]
    request = {
        "endpoint": str(endpoint),
        "requests": [
            {"endpoint": response.endpoint, "params": response.params}
            for response in ordered
        ],
    }
    values: list[dict[str, Any]] = []
    for index, response in enumerate(ordered):
        for entry in _scalar_values(
            response.records,
            key_prefix=f"responses[{index}].",
        ):
            entry["response_endpoint"] = response.endpoint
            values.append(entry)
    return _descriptor(
        endpoint,
        request_sha256=_sha256(request),
        response_sha256=_sha256([list(response.records) for response in ordered]),
        values=values,
    )


class VendorResponseLog:
    """Thread-safe record of the provider responses one tool call used.

    A log opened inside another (a tool built on another tool) also records
    into the enclosing log, so the outer tool's descriptor covers both.
    """

    def __init__(self, parent: VendorResponseLog | _UnscopedLog) -> None:
        self._parent = parent
        self._lock = threading.Lock()
        self._responses: list[VendorResponse] = []

    def record(self, endpoint: str, params: Mapping[str, Any], response: Any) -> None:
        self.extend(
            (
                VendorResponse(
                    endpoint=str(endpoint),
                    params=_json_value(dict(params)),
                    records=_normalized_response(_response_records(response)),
                ),
            )
        )

    def extend(self, responses: Sequence[VendorResponse]) -> None:
        with self._lock:
            self._responses.extend(responses)
        self._parent.extend(responses)

    @property
    def responses(self) -> tuple[VendorResponse, ...]:
        with self._lock:
            return tuple(self._responses)

    def descriptor(self, endpoint: str) -> dict[str, Any]:
        return build_composite_vendor_response_descriptor(endpoint, self.responses)


class _UnscopedLog:
    """Responses fetched outside any tool call belong to no tool descriptor."""

    def record(self, endpoint: str, params: Mapping[str, Any], response: Any) -> None:
        pass

    def extend(self, responses: Sequence[VendorResponse]) -> None:
        pass


_ACTIVE_RESPONSE_LOG: ContextVar[VendorResponseLog | _UnscopedLog] = ContextVar(
    "fmp_vendor_response_log",
    default=_UnscopedLog(),
)


def active_vendor_response_log() -> VendorResponseLog | _UnscopedLog:
    """Return the log provider responses are recorded into in this context.

    Worker threads do not inherit it: submit work through
    ``contextvars.copy_context().run`` so their fetches are recorded.
    """

    return _ACTIVE_RESPONSE_LOG.get()


def record_vendor_response(
    endpoint: str,
    params: Mapping[str, Any],
    response: Any,
) -> None:
    """Record one provider response (DataFrame, record list, or object)."""

    _ACTIVE_RESPONSE_LOG.get().record(endpoint, params, response)


@contextmanager
def vendor_response_log() -> Iterator[VendorResponseLog]:
    """Open the response log for one tool call."""

    log = VendorResponseLog(_ACTIVE_RESPONSE_LOG.get())
    token = _ACTIVE_RESPONSE_LOG.set(log)
    try:
        yield log
    finally:
        _ACTIVE_RESPONSE_LOG.reset(token)


_ToolResult = TypeVar("_ToolResult", bound=dict)


def returns_vendor_lineage(
    endpoint: str,
) -> Callable[[Callable[..., _ToolResult]], Callable[..., _ToolResult]]:
    """Attach the call's composite ``lineage_descriptor`` to a tool's result."""

    def decorate(fn: Callable[..., _ToolResult]) -> Callable[..., _ToolResult]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> _ToolResult:
            with vendor_response_log() as log:
                result = fn(*args, **kwargs)
            result["lineage_descriptor"] = log.descriptor(endpoint)
            return result

        return wrapper

    return decorate


def build_vendor_request_sha256(
    endpoint: str,
    params: Mapping[str, Any],
) -> str:
    """Hash the exact endpoint and post-validation request parameters."""

    request = {"endpoint": str(endpoint), "params": _json_value(dict(params))}
    return _sha256(request)


def dataframe_records(dataframe: pd.DataFrame) -> tuple[dict[str, Any], ...]:
    """Project a normalized provider DataFrame into detached record mappings."""

    return tuple(dict(record) for record in dataframe.to_dict(orient="records"))


@dataclass(frozen=True, slots=True, init=False, eq=False)
class FMPFetchResult:
    """One normalized FMP response with provider-issued lineage."""

    endpoint: str
    source: FMPFetchSource
    observed_at: datetime
    _validated_params: dict[str, Any] = dataclass_field(repr=False)
    _dataframe: pd.DataFrame = dataclass_field(repr=False)
    _normalized_records: tuple[dict[str, Any], ...] = dataclass_field(repr=False)
    _canonical_response_bytes: bytes = dataclass_field(repr=False)
    _lineage_descriptor: dict[str, Any] = dataclass_field(repr=False)

    def __init__(
        self,
        endpoint: str,
        validated_params: Mapping[str, Any],
        dataframe: pd.DataFrame,
        source: FMPFetchSource,
        observed_at: datetime,
        lineage_descriptor: Mapping[str, Any],
    ) -> None:
        if not endpoint:
            raise ValueError("endpoint must not be empty")
        if source not in ("live", "cache"):
            raise ValueError("source must be 'live' or 'cache'")
        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError("dataframe must be a pandas DataFrame")
        offset = observed_at.utcoffset() if observed_at.tzinfo is not None else None
        if offset != timedelta(0):
            raise ValueError("observed_at must be an aware UTC datetime")

        params_copy = deepcopy(dict(validated_params))
        dataframe_copy = dataframe.copy(deep=True)
        normalized_records = _normalized_response(dataframe_records(dataframe_copy))
        canonical_response_bytes = _canonical_json_bytes(normalized_records)
        descriptor = deepcopy(dict(lineage_descriptor))
        if descriptor.get("endpoint") != endpoint:
            raise ValueError("lineage descriptor endpoint must match result endpoint")
        if not isinstance(descriptor.get("response_sha256"), str):
            raise ValueError("lineage descriptor must include response_sha256")
        if not isinstance(descriptor.get("values"), list):
            raise ValueError("lineage descriptor must include scalar values")
        expected_descriptor = _descriptor_from_normalized_response(
            endpoint,
            params_copy,
            normalized_records,
        )
        if descriptor != expected_descriptor:
            raise ValueError(
                "lineage descriptor must match validated params and dataframe"
            )

        object.__setattr__(self, "endpoint", endpoint)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "_validated_params", params_copy)
        object.__setattr__(self, "_dataframe", dataframe_copy)
        object.__setattr__(self, "_normalized_records", normalized_records)
        object.__setattr__(
            self,
            "_canonical_response_bytes",
            canonical_response_bytes,
        )
        object.__setattr__(self, "_lineage_descriptor", descriptor)

    @property
    def validated_params(self) -> Mapping[str, Any]:
        """Return a detached, read-only view of the validated request params."""

        return MappingProxyType(deepcopy(self._validated_params))

    @property
    def dataframe(self) -> pd.DataFrame:
        """Return a defensive copy of the normalized provider response."""

        return self._dataframe.copy(deep=True)

    @property
    def lineage_descriptor(self) -> Mapping[str, Any]:
        """Return a detached, read-only view of the provider descriptor."""

        return MappingProxyType(deepcopy(self._lineage_descriptor))

    @property
    def records(self) -> tuple[dict[str, Any], ...]:
        """Return detached normalized records without exposing mutable state."""

        return dataframe_records(self._dataframe)

    @property
    def normalized_records(self) -> tuple[dict[str, Any], ...]:
        """Return the detached JSON-normalized response hashed by lineage."""

        return tuple(deepcopy(record) for record in self._normalized_records)

    @property
    def canonical_response_bytes(self) -> bytes:
        """Return the exact canonical response bytes hashed by lineage."""

        return self._canonical_response_bytes

    @property
    def response_sha256(self) -> str:
        """Return the normalized-response digest issued by the provider boundary."""

        return str(self._lineage_descriptor["response_sha256"])

    @property
    def scalar_coordinates(self) -> tuple[dict[str, Any], ...]:
        """Return detached scalar coordinates from the issued descriptor."""

        values = self._lineage_descriptor["values"]
        return tuple(deepcopy(dict(value)) for value in values)


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            return None
        return format(value, "f")
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(child) for key, child in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_json_value(child) for child in value]
    if hasattr(value, "tolist"):
        # numpy scalars, and list fields the parquet cache reads back as arrays.
        return _json_value(value.tolist())
    if hasattr(value, "item"):
        return _json_value(value.item())
    raise TypeError(f"unsupported FMP lineage value: {type(value).__name__}")


def _normalized_response(
    records: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], ...]:
    normalized: list[dict[str, Any]] = []
    for record in records:
        value = _json_value(dict(record))
        if not isinstance(value, dict):
            raise TypeError("normalized FMP response record must be an object")
        normalized.append(value)
    return tuple(normalized)


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
            return None
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
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")


def _response_records(response: Any) -> Sequence[Mapping[str, Any]]:
    if isinstance(response, pd.DataFrame):
        return dataframe_records(response)
    if isinstance(response, Mapping):
        return (response,)
    return tuple(response)


__all__ = [
    "FMPFetchResult",
    "FMPFetchSource",
    "SCHEMA_VERSION",
    "VendorResponse",
    "VendorResponseLog",
    "active_vendor_response_log",
    "build_composite_vendor_response_descriptor",
    "build_vendor_request_sha256",
    "build_vendor_response_descriptor",
    "dataframe_records",
    "record_vendor_response",
    "returns_vendor_lineage",
    "vendor_response_log",
]
