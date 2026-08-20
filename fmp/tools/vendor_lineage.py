"""Compatibility exports for the provider-owned FMP lineage boundary."""

from ..lineage import (
    SCHEMA_VERSION,
    _canonical_scalar as _canonical_scalar,
    _json_value as _json_value,
    build_vendor_response_descriptor,
)


__all__ = ["SCHEMA_VERSION", "build_vendor_response_descriptor"]
