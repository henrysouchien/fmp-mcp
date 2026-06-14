"""Dispatch comps manifest source bindings against FMP response bundles."""

from __future__ import annotations

import ast
import operator
from datetime import date
from types import SimpleNamespace
from typing import Any, NamedTuple


class DispatchResult(NamedTuple):
    value: Any
    source_endpoint: str | None
    source_meta: dict[str, Any] | None


_FIELD_ALIASES: dict[tuple[str, str], tuple[str, ...]] = {
    ("ratios_ttm", "priceEarningsRatioTTM"): (
        "priceEarningsRatioTTM",
        "priceToEarningsRatioTTM",
    ),
    ("enterprise_values_ttm", "evToSales"): (
        "evToSales",
        "evToSalesTTM",
        "enterpriseValueToRevenue",
    ),
    ("enterprise_values_ttm", "evToEBITDA"): (
        "evToEBITDA",
        "evToEbitdaTTM",
        "enterpriseValueMultipleTTM",
    ),
    ("cash_flow_ttm", "netDividendsPaid"): (
        "netDividendsPaid",
        "commonDividendsPaid",
    ),
}

_ANALYST_FIELD_MAP: dict[str, tuple[str, int]] = {
    "estimatedEpsAvg_fy1": ("epsAvg", 1),
    "estimatedEpsAvg_fy2": ("epsAvg", 2),
    "estimatedEpsAvg_fy3": ("epsAvg", 3),
    "estimatedRevenueAvg_ntm": ("revenueAvg", 1),
    "estimatedEbitdaAvg_ntm": ("ebitdaAvg", 1),
}

_BINARY_OPERATORS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Pow: operator.pow,
}
_UNARY_OPERATORS = {
    ast.UAdd: operator.pos,
    ast.USub: operator.neg,
}


def dispatch_source_binding(
    binding: Any,
    fmp_response_bundle: dict[str, Any],
    focal_ticker: str,
    *,
    resolved_metrics: dict[str, Any] | None = None,
    metric_key: str | None = None,
    resolving: set[str] | None = None,
    fiscal_year: int | None = None,
    kpi_registry: Any | None = None,
) -> DispatchResult:
    """Resolve one manifest source binding against raw per-endpoint payloads."""

    kind = _binding_attr(binding, "kind")
    if kind == "fmp_endpoint":
        endpoint = _binding_attr(binding, "fmp_endpoint")
        field = _binding_attr(binding, "fmp_field")
        if not endpoint or not field:
            return DispatchResult(None, None, None)
        return DispatchResult(
            _extract_fmp_endpoint_value(
                str(endpoint),
                str(field),
                fmp_response_bundle,
            ),
            str(endpoint),
            None,
        )

    if kind == "derived":
        formula = _binding_attr(binding, "derived_formula")
        if not formula:
            return DispatchResult(None, None, None)
        return DispatchResult(
            _evaluate_derived_formula(
                str(formula),
                fmp_response_bundle,
                resolved_metrics or {},
                metric_key=metric_key,
                resolving=resolving or set(),
            ),
            None,
            None,
        )

    if kind == "kpi":
        _require_fiscal_year(fiscal_year, str(kind))
        if kpi_registry is None:
            raise ValueError("kpi source requires kpi_registry")
        kpi_key = _require_binding_value(binding, "kpi_key", "kpi source requires kpi_key")
        kpi_definition = _lookup_kpi(kpi_registry, kpi_key)
        extraction = _kpi_extraction(kpi_definition)
        extraction_kind = _extraction_attr(extraction, "kind")
        if extraction_kind == "transcript_kpi":
            _require_extraction_list(
                extraction,
                "pattern_hints",
                f"KPI '{kpi_key}' transcript_kpi extraction requires pattern_hints",
            )
            synthetic_binding = SimpleNamespace(kind="transcript_kpi", kpi_key=kpi_key)
        elif extraction_kind == "edgar_concept":
            concept = _require_extraction_value(
                extraction,
                "concept_name",
                f"KPI '{kpi_key}' edgar_concept extraction requires concept_name",
            )
            synthetic_binding = SimpleNamespace(kind="edgar_concept", edgar_concept=concept)
        elif extraction_kind == "derived":
            formula = _require_extraction_value(
                extraction,
                "formula",
                f"KPI '{kpi_key}' derived extraction requires formula",
            )
            synthetic_binding = SimpleNamespace(kind="derived", derived_formula=formula)
        else:
            raise ValueError(f"unsupported KPI extraction kind: {extraction_kind}")
        return dispatch_source_binding(
            synthetic_binding,
            fmp_response_bundle,
            focal_ticker,
            resolved_metrics=resolved_metrics,
            metric_key=metric_key,
            resolving=resolving,
            fiscal_year=fiscal_year,
            kpi_registry=kpi_registry,
        )

    if kind == "edgar_concept":
        _require_fiscal_year(fiscal_year, str(kind))
        concept = _require_binding_value(
            binding,
            "edgar_concept",
            "edgar_concept source requires edgar_concept",
        )
        from fmp.tools import edgar_concept_fetcher

        value, raw_payload, retrieved_at = edgar_concept_fetcher.fetch_concept(
            focal_ticker,
            concept,
            int(fiscal_year),
        )
        source = raw_payload.get("source") if isinstance(raw_payload, dict) else None
        source_meta = {
            "accession": _source_metadata_value(source, "accession", "accessionNumber"),
            "form": _source_metadata_value(source, "form", "filing_type", "filingType"),
            "fiscal_year": int(fiscal_year),
            "concept": concept,
            "retrieved_at": retrieved_at,
            "raw_payload": raw_payload,
        }
        return DispatchResult(value, f"edgar_concept:{concept}", source_meta)

    if kind == "transcript_kpi":
        _require_fiscal_year(fiscal_year, str(kind))
        if kpi_registry is None:
            raise ValueError("transcript_kpi source requires kpi_registry")
        kpi_key = _require_binding_value(
            binding,
            "kpi_key",
            "transcript_kpi source requires kpi_key",
        )
        kpi_definition = _lookup_kpi(kpi_registry, kpi_key)
        extraction = _kpi_extraction(kpi_definition)
        _require_extraction_list(
            extraction,
            "pattern_hints",
            f"KPI '{kpi_key}' transcript_kpi extraction requires pattern_hints",
        )
        from fmp.tools import transcript_kpi_fetcher

        value, raw_payload, retrieved_at = (
            transcript_kpi_fetcher.fetch_kpi_from_transcripts(
                focal_ticker,
                kpi_key,
                kpi_definition,
                int(fiscal_year),
            )
        )
        source_meta = None
        if value is not None and isinstance(raw_payload, dict):
            source_meta = {
                "quarter": raw_payload.get("quarter"),
                "fiscal_year": int(fiscal_year),
                "kpi_key": kpi_key,
                "matched_excerpt": raw_payload.get("matched_excerpt"),
                "retrieved_at": retrieved_at,
            }
        return DispatchResult(value, f"transcript_kpi:{kpi_key}", source_meta)

    raise ValueError(f"unsupported comps source kind: {kind}")


def _binding_attr(binding: Any, key: str) -> Any:
    if isinstance(binding, dict):
        return binding.get(key)
    return getattr(binding, key, None)


def _require_fiscal_year(fiscal_year: int | None, kind: str) -> None:
    if fiscal_year is None:
        raise ValueError(f"{kind} source requires fiscal_year")


def _require_binding_value(binding: Any, key: str, message: str) -> str:
    value = _binding_attr(binding, key)
    if value is None or str(value).strip() == "":
        raise ValueError(message)
    return str(value)


def _lookup_kpi(kpi_registry: Any, kpi_key: str) -> Any:
    kpis = _registry_kpis(kpi_registry)
    for kpi in kpis:
        if _kpi_attr(kpi, "key") == kpi_key:
            return kpi
    raise ValueError(f"kpi_key not found in registry: {kpi_key}")


def _registry_kpis(kpi_registry: Any) -> list[Any]:
    if isinstance(kpi_registry, dict):
        kpis = kpi_registry.get("kpis")
    else:
        kpis = getattr(kpi_registry, "kpis", None)
    if not isinstance(kpis, list):
        raise ValueError("kpi_registry must expose a kpis list")
    return kpis


def _kpi_attr(kpi_definition: Any, key: str) -> Any:
    if isinstance(kpi_definition, dict):
        return kpi_definition.get(key)
    return getattr(kpi_definition, key, None)


def _kpi_extraction(kpi_definition: Any) -> Any:
    extraction = _kpi_attr(kpi_definition, "extraction")
    if extraction is None:
        raise ValueError(f"KPI '{_kpi_attr(kpi_definition, 'key')}' requires extraction")
    return extraction


def _extraction_attr(extraction: Any, key: str) -> Any:
    if isinstance(extraction, dict):
        return extraction.get(key)
    return getattr(extraction, key, None)


def _require_extraction_value(extraction: Any, key: str, message: str) -> str:
    value = _extraction_attr(extraction, key)
    if value is None or str(value).strip() == "":
        raise ValueError(message)
    return str(value)


def _require_extraction_list(extraction: Any, key: str, message: str) -> list[str]:
    value = _extraction_attr(extraction, key)
    if not isinstance(value, list) or not value:
        raise ValueError(message)
    return [str(item) for item in value]


def _source_metadata_value(source: Any, *keys: str) -> Any:
    if not isinstance(source, dict):
        return None
    for key in keys:
        value = source.get(key)
        if value is not None:
            return value
    for child_key in ("filing", "metadata", "source"):
        child = source.get(child_key)
        value = _source_metadata_value(child, *keys)
        if value is not None:
            return value
    return None


def _extract_fmp_endpoint_value(
    endpoint: str,
    field: str,
    bundle: dict[str, Any],
) -> Any:
    if endpoint == "analyst_estimates":
        return _extract_analyst_estimate(field, bundle)

    row = _first_record(bundle.get(endpoint))
    for key in _field_keys(endpoint, field):
        if key in row and row.get(key) is not None:
            return row.get(key)
    return None


def _field_keys(endpoint: str, field: str) -> tuple[str, ...]:
    return _FIELD_ALIASES.get((endpoint, field), (field,))


def _extract_analyst_estimate(field: str, bundle: dict[str, Any]) -> Any:
    mapped = _ANALYST_FIELD_MAP.get(field)
    if mapped is None:
        row = _first_record(bundle.get("analyst_estimates"))
        return row.get(field)

    source_field, fiscal_year_index = mapped
    rows = _future_estimate_rows(
        bundle.get("analyst_estimates"),
        _metadata_value(bundle, "last_reported_fiscal_date"),
    )
    if len(rows) < fiscal_year_index:
        return None
    return rows[fiscal_year_index - 1].get(source_field)


def _future_estimate_rows(
    payload: Any,
    last_reported_fiscal_date: Any,
) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        rows = [payload]
    elif isinstance(payload, list):
        rows = [row for row in payload if isinstance(row, dict)]
    else:
        rows = []

    cutoff = _parse_date(last_reported_fiscal_date)
    dated_rows: list[tuple[date, dict[str, Any]]] = []
    undated_rows: list[dict[str, Any]] = []
    for row in rows:
        parsed = _parse_date(row.get("date"))
        if parsed is None:
            undated_rows.append(row)
            continue
        if cutoff is not None and parsed <= cutoff:
            continue
        dated_rows.append((parsed, row))

    ordered = [row for _, row in sorted(dated_rows, key=lambda item: item[0])]
    return ordered + undated_rows


def _metadata_value(bundle: dict[str, Any], key: str) -> Any:
    metadata = bundle.get("_metadata")
    if isinstance(metadata, dict):
        return metadata.get(key)
    return None


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _first_record(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                return item
    return {}


def _evaluate_derived_formula(
    formula: str,
    bundle: dict[str, Any],
    resolved_metrics: dict[str, Any],
    *,
    metric_key: str | None,
    resolving: set[str],
) -> Any:
    expression = ast.parse(formula, mode="eval")
    referenced_names = {
        node.id for node in ast.walk(expression) if isinstance(node, ast.Name)
    }
    if metric_key and metric_key in referenced_names:
        raise ValueError(f"cyclic derived metric dependency: {metric_key}")
    cyclic = referenced_names.intersection(resolving)
    if cyclic:
        raise ValueError(f"cyclic derived metric dependency: {sorted(cyclic)}")

    variables = _raw_endpoint_variables(bundle)
    variables.update(resolved_metrics)
    variables.setdefault("price", _first_record(bundle.get("profile")).get("price"))
    try:
        return _eval_ast(expression.body, variables)
    except (TypeError, ValueError, ZeroDivisionError, OverflowError):
        return None


def _raw_endpoint_variables(bundle: dict[str, Any]) -> dict[str, Any]:
    variables: dict[str, Any] = {}
    for endpoint, payload in bundle.items():
        if str(endpoint).startswith("_"):
            continue
        row = _first_record(payload)
        endpoint_prefix = str(endpoint).replace("-", "_")
        for field, value in row.items():
            field_name = str(field)
            if field_name.isidentifier():
                variables.setdefault(field_name, value)
            endpoint_field = f"{endpoint_prefix}__{field_name}"
            if endpoint_field.isidentifier():
                variables[endpoint_field] = value
    return variables


def _eval_ast(node: ast.AST, variables: dict[str, Any]) -> float:
    if isinstance(node, ast.Constant):
        if isinstance(node.value, bool) or not isinstance(node.value, (int, float)):
            raise ValueError("derived constants must be numeric")
        return float(node.value)

    if isinstance(node, ast.Name):
        value = variables.get(node.id)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"derived variable is not numeric: {node.id}")
        return float(value)

    if isinstance(node, ast.BinOp):
        operator_fn = _BINARY_OPERATORS.get(type(node.op))
        if operator_fn is None:
            raise ValueError(f"unsupported derived operator: {type(node.op).__name__}")
        return operator_fn(_eval_ast(node.left, variables), _eval_ast(node.right, variables))

    if isinstance(node, ast.UnaryOp):
        operator_fn = _UNARY_OPERATORS.get(type(node.op))
        if operator_fn is None:
            raise ValueError(f"unsupported derived operator: {type(node.op).__name__}")
        return operator_fn(_eval_ast(node.operand, variables))

    raise ValueError(f"unsupported derived expression: {type(node).__name__}")


__all__ = ["DispatchResult", "dispatch_source_binding"]
