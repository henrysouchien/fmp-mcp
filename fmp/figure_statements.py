"""What an FMP figure measures, stated as data by this package, once.

Ruling ``EVERY-FIGURE-STATES-ITS-WORLD-2026-11-18`` (Henry, 2026-11-18, of the
mechanism: "approved"): the vendor's facts about its own figures are stated in
one place, here, and every tool that hands on an FMP figure (plus the add-in's
FMP adapter) reads them from here. Every statement carries ``{measure, period,
basis}`` under the shared wire contract in ``value_semantics_core.figure_statement``.
A fact that cannot be filled is ``not_stated(reason)``, never a guess, a bare
string, null or a missing key.
"""

from __future__ import annotations

from datetime import date, datetime
import json
import math
import re
from types import MappingProxyType
from typing import Any, Callable, Literal, Mapping, Sequence

from pydantic import ValidationError
from value_semantics_core import FiscalYearPeriod
from value_semantics_core.figure_statement import (
    Basis,
    FigureStatement,
    FiscalPeriod,
    NotStated,
    NotStatedReason,
    Place,
    StatementEntry,
    TrailingPeriod,
    VENDOR_FIELD_CLASSIFICATIONS,
    VendorFieldClassification,
    expand_place_key,
    expand_same_as_figure,
    not_stated,
    parse_statements,
    render_statement_label,
    render_statements,
    resolve_statements,
    trailing_alignment,
    validate_statement_entries,
)

from .definitions import FMPFieldDefinitionAvailable, get_fmp_field_definition
from .lineage import _canonical_scalar, _normalized_response


def _ns(reason: NotStatedReason) -> dict[str, str]:
    return not_stated(reason).model_dump(mode="json")


VENDOR_UNCOMPARED_BASIS: Mapping[str, str] = MappingProxyType(
    _ns("vendor_figure_not_compared_to_filing")
)
BASIS_NOT_RECORDED: Mapping[str, str] = MappingProxyType(_ns("basis_not_recorded"))

# Canonical vendor field classification table (W1 single owner, moved from
# ai-excel-addin/schema/provider_port/adapters/fmp/mapping_config.json).
FMP_FIELD_CLASSIFICATIONS: Mapping[tuple[str, str], VendorFieldClassification] = MappingProxyType(
    {
        ("balance_sheet", "accountPayables"): "filing_line",
        ("balance_sheet", "accruedExpenses"): "filing_line",
        ("balance_sheet", "accumulatedOtherComprehensiveIncomeLoss"): "filing_line",
        ("balance_sheet", "additionalPaidInCapital"): "filing_line",
        ("balance_sheet", "capitalLeaseObligationsCurrent"): "filing_line",
        ("balance_sheet", "capitalLeaseObligationsNonCurrent"): "filing_line",
        ("balance_sheet", "cashAndCashEquivalents"): "filing_line",
        ("balance_sheet", "cashAndShortTermInvestments"): "filing_line",
        ("balance_sheet", "commonStock"): "filing_line",
        ("balance_sheet", "deferredRevenue"): "filing_line",
        ("balance_sheet", "deferredRevenueNonCurrent"): "filing_line",
        ("balance_sheet", "deferredTaxLiabilitiesNonCurrent"): "filing_line",
        ("balance_sheet", "goodwill"): "filing_line",
        ("balance_sheet", "intangibleAssets"): "filing_line",
        ("balance_sheet", "inventory"): "filing_line",
        ("balance_sheet", "longTermDebt"): "filing_line",
        ("balance_sheet", "netReceivables"): "filing_line",
        ("balance_sheet", "otherNonCurrentAssets"): "filing_line",
        ("balance_sheet", "otherNonCurrentLiabilities"): "filing_line",
        ("balance_sheet", "prepaids"): "filing_line",
        ("balance_sheet", "propertyPlantEquipmentNet"): "filing_line",
        ("balance_sheet", "retainedEarnings"): "filing_line",
        ("balance_sheet", "shortTermDebt"): "filing_line",
        ("balance_sheet", "taxAssets"): "filing_line",
        ("balance_sheet", "totalAssets"): "filing_line",
        ("balance_sheet", "totalCurrentAssets"): "filing_line",
        ("balance_sheet", "totalCurrentLiabilities"): "filing_line",
        ("balance_sheet", "totalDebt"): "vendor_convenience",
        ("balance_sheet", "totalLiabilities"): "filing_line",
        ("balance_sheet", "totalStockholdersEquity"): "filing_line",
        ("balance_sheet", "treasuryStock"): "filing_line",
        ("cash_flow", "accountsPayables"): "filing_line",
        ("cash_flow", "accountsReceivables"): "filing_line",
        ("cash_flow", "acquisitionsNet"): "filing_line",
        ("cash_flow", "capitalExpenditure"): "filing_line",
        ("cash_flow", "cashAtEndOfPeriod"): "filing_line",
        ("cash_flow", "commonDividendsPaid"): "filing_line",
        ("cash_flow", "commonStockIssuance"): "filing_line",
        ("cash_flow", "commonStockRepurchased"): "filing_line",
        ("cash_flow", "deferredIncomeTax"): "filing_line",
        ("cash_flow", "depreciationAndAmortization"): "filing_line",
        ("cash_flow", "effectOfForexChangesOnCash"): "filing_line",
        ("cash_flow", "freeCashFlow"): "aggregate_derivation",
        ("cash_flow", "inventory"): "filing_line",
        ("cash_flow", "longTermNetDebtIssuance"): "aggregate_derivation",
        ("cash_flow", "netCashProvidedByFinancingActivities"): "filing_line",
        ("cash_flow", "netCashProvidedByInvestingActivities"): "filing_line",
        ("cash_flow", "operatingCashFlow"): "filing_line",
        ("cash_flow", "otherFinancingActivities"): "filing_line",
        ("cash_flow", "otherInvestingActivities"): "filing_line",
        ("cash_flow", "otherNonCashItems"): "filing_line",
        ("cash_flow", "otherWorkingCapital"): "filing_line",
        ("cash_flow", "purchasesOfInvestments"): "filing_line",
        ("cash_flow", "salesMaturitiesOfInvestments"): "filing_line",
        ("cash_flow", "stockBasedCompensation"): "filing_line",
        ("income_statement", "costOfRevenue"): "filing_line",
        ("income_statement", "ebitda"): "filing_line",
        ("income_statement", "eps"): "filing_line",
        ("income_statement", "epsDiluted"): "filing_line",
        ("income_statement", "generalAndAdministrativeExpenses"): "filing_line",
        ("income_statement", "grossProfit"): "filing_line",
        ("income_statement", "incomeBeforeTax"): "filing_line",
        ("income_statement", "incomeTaxExpense"): "filing_line",
        ("income_statement", "interestExpense"): "filing_line",
        ("income_statement", "interestIncome"): "filing_line",
        ("income_statement", "netIncome"): "filing_line",
        ("income_statement", "operatingExpenses"): "filing_line",
        ("income_statement", "operatingIncome"): "filing_line",
        ("income_statement", "researchAndDevelopmentExpenses"): "filing_line",
        ("income_statement", "revenue"): "filing_line",
        ("income_statement", "sellingAndMarketingExpenses"): "filing_line",
        ("income_statement", "sellingGeneralAndAdministrativeExpenses"): "filing_line",
        ("income_statement", "totalOtherIncomeExpensesNet"): "filing_line",
        ("income_statement", "weightedAverageShsOut"): "filing_line",
        ("income_statement", "weightedAverageShsOutDil"): "filing_line",
        ("key_metrics", "workingCapital"): "vendor_convenience",
        ("manual", "default"): "market_observation",
        ("profile", "beta"): "market_observation",
        ("quote", "price"): "market_observation",
    }
)

_ISO_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")


def fmp_field_classification(
    endpoint: str, field: str
) -> VendorFieldClassification | dict[str, str]:
    """Return the recorded vendor classification for ``(endpoint, field)``, or not-stated."""

    recorded = FMP_FIELD_CLASSIFICATIONS.get((endpoint, field))
    if recorded is not None:
        return recorded
    return _ns("classification_not_recorded")


def vendor_basis(endpoint: str) -> Basis | dict[str, str]:
    """Return the canonical accounting basis for one FMP endpoint's figures."""

    if endpoint == "analyst_estimates":
        return "street_estimate"
    return dict(VENDOR_UNCOMPARED_BASIS)


def fmp_row_currency(row: Mapping[str, Any], *, field: str = "reportedCurrency") -> dict[str, str]:
    """Parse one FMP row's ISO-4217 currency code without guessing USD."""

    raw = row.get(field)
    if raw is None:
        return {"kind": "not_stated", "reason": "vendor_row_has_no_reported_currency"}
    if isinstance(raw, bool):
        raise ValueError(f"{field} must not be boolean")
    if isinstance(raw, float) and math.isnan(raw):
        return {"kind": "not_stated", "reason": "vendor_row_has_no_reported_currency"}
    text = str(raw).strip()
    if not text:
        return {"kind": "not_stated", "reason": "vendor_row_has_no_reported_currency"}
    code = text.upper()
    if _ISO_CURRENCY_RE.fullmatch(code) is None:
        raise ValueError(f"invalid ISO currency code in {field}: {raw!r}")
    return {"kind": "stated", "code": code}


def fmp_record_fiscal_year(row: Mapping[str, Any]) -> int | None:
    """Canonical fiscal year recorded on an FMP row, or ``None`` when unrecorded.

    Malformed present values raise ``ValueError`` (never silently coerced).
    """

    raw = row.get("fiscalYear")
    if raw is None:
        raw = row.get("calendarYear")
    if raw is None:
        return None
    if isinstance(raw, bool):
        raise ValueError("fiscalYear must not be boolean")
    if isinstance(raw, float):
        if math.isnan(raw):
            return None
        if not math.isfinite(raw) or not raw.is_integer():
            raise ValueError(f"invalid fiscalYear: {raw!r}")
        raw = int(raw)
    text = str(raw).strip()
    if not text:
        return None
    try:
        return FiscalYearPeriod(kind="fiscal_year", fiscal_year=int(text)).fiscal_year
    except (ValueError, ValidationError) as exc:
        raise ValueError(f"invalid fiscalYear: {raw!r}") from exc


def fmp_record_period_end(row: Mapping[str, Any]) -> date | None:
    """Canonical period-end date recorded on an FMP row, or ``None`` when unrecorded.

    Malformed present values raise ``ValueError``.
    """

    raw = row.get("date")
    if raw is None:
        raw = row.get("fiscalDateEnding")
    if raw is None:
        return None
    if isinstance(raw, bool):
        raise ValueError("date must not be boolean")
    if isinstance(raw, float) and math.isnan(raw):
        return None
    text = str(raw).strip()
    if not text:
        return None
    return date.fromisoformat(text[:10])


def vendor_measure(endpoint: str, field: str, *, observed_at: datetime) -> dict[str, Any]:
    """FMP's own field, with its definition and classification."""

    definition = get_fmp_field_definition(
        endpoint,
        field,
        response_observed_at=observed_at,
    )
    return {
        "kind": "vendor",
        "provider": "fmp",
        "endpoint": endpoint,
        "field": field,
        "definition": (
            {
                "kind": "recorded",
                "artifact_sha256": definition.artifact_sha256,
                "semantic": definition.semantic,
            }
            if isinstance(definition, FMPFieldDefinitionAvailable)
            else _ns("definition_not_recorded")
        ),
        "classification": fmp_field_classification(endpoint, field),
    }


PeriodRule = Literal["fiscal_row", "vendor_trailing", "estimate"]

_PERIOD_RULES: Mapping[str, PeriodRule] = MappingProxyType(
    {
        "income_statement": "fiscal_row",
        "balance_sheet": "fiscal_row",
        "cash_flow": "fiscal_row",
        "key_metrics": "fiscal_row",
        "ratios": "fiscal_row",
        "revenue_product_segmentation": "fiscal_row",
        "key_metrics_ttm": "vendor_trailing",
        "ratios_ttm": "vendor_trailing",
        "analyst_estimates": "estimate",
    }
)

_QUARTER_LABELS: Mapping[str, int | None] = MappingProxyType(
    {"FY": None, "Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}
)

_PERIOD_IDENTITY_FIELDS = frozenset({"fiscalYear", "calendarYear"})


def row_period(
    endpoint: str,
    params: Mapping[str, Any],
    row: Mapping[str, Any],
    *,
    observed_at: datetime,
) -> dict[str, Any]:
    """The period every figure on one row of ``endpoint`` shares."""

    rule = _PERIOD_RULES.get(endpoint)
    if rule == "vendor_trailing":
        return {
            "kind": "trailing",
            "periods_summed": _ns("periods_not_recorded"),
            "end_date": _ns("end_date_not_recorded"),
            "status": "actual",
        }
    if rule == "fiscal_row":
        raw_period = row.get("period")
        if isinstance(raw_period, bool):
            raise ValueError("period must not be boolean")
        label = str(raw_period or "").strip().upper()
        if label in _QUARTER_LABELS:
            return _fiscal_period(row, quarter=_QUARTER_LABELS[label], status="actual")
        return _ns("period_not_recorded")
    if rule == "estimate" and params.get("period") in ("annual", "quarter"):
        raw_row_period = row.get("period")
        if isinstance(raw_row_period, bool):
            raise ValueError("period must not be boolean")
        row_label = (
            ""
            if raw_row_period is None or (isinstance(raw_row_period, float) and math.isnan(raw_row_period))
            else str(raw_row_period).strip().upper()
        )
        if params["period"] == "annual":
            if row_label and row_label not in ("FY", "ANNUAL"):
                return _ns("period_not_recorded")
            quarter: int | dict[str, str] | None = None
        else:
            if not row_label:
                quarter = _ns("quarter_not_recorded")
            elif row_label in ("Q1", "Q2", "Q3", "Q4"):
                quarter = _QUARTER_LABELS[row_label]
            else:
                return _ns("period_not_recorded")
        period = _fiscal_period(row, quarter=quarter, status="projected")
        date_is = get_fmp_field_definition(endpoint, "date", response_observed_at=observed_at)
        if not (
            isinstance(date_is, FMPFieldDefinitionAvailable)
            and date_is.semantic == "target_period_end"
        ):
            period["end_date"] = _ns("end_date_not_recorded")
        return period
    return _ns("period_not_recorded")


def _fiscal_period(
    row: Mapping[str, Any],
    *,
    quarter: int | dict[str, str] | None,
    status: Literal["actual", "projected"],
) -> dict[str, Any]:
    raw_fy = row.get("fiscalYear")
    if raw_fy is None or (isinstance(raw_fy, float) and math.isnan(raw_fy)) or (
        isinstance(raw_fy, str) and not raw_fy.strip()
    ):
        fiscal_year: int | dict[str, str] = _ns("fiscal_year_not_recorded")
    else:
        parsed_fy = fmp_record_fiscal_year({"fiscalYear": raw_fy})
        fiscal_year = parsed_fy if parsed_fy is not None else _ns("fiscal_year_not_recorded")
    raw_date = row.get("date")
    if raw_date is None or (isinstance(raw_date, float) and math.isnan(raw_date)) or (
        isinstance(raw_date, str) and not raw_date.strip()
    ):
        end_date: str | dict[str, str] = _ns("end_date_not_recorded")
    else:
        parsed_end = fmp_record_period_end({"date": raw_date})
        end_date = parsed_end.isoformat() if parsed_end is not None else _ns("end_date_not_recorded")
    return {
        "kind": "fiscal_period",
        "fiscal_year": fiscal_year,
        "quarter": quarter,
        "end_date": end_date,
        "status": status,
        "built": "as_reported",
    }


def vendor_figure_statement(
    endpoint: str,
    params: Mapping[str, Any],
    row: Mapping[str, Any],
    field: str,
    *,
    observed_at: datetime,
) -> dict[str, Any]:
    """Full three-fact ``FigureStatement`` wire dict for one vendor scalar."""

    statement = {
        "measure": vendor_measure(endpoint, field, observed_at=observed_at),
        "period": row_period(endpoint, params, row, observed_at=observed_at),
        "basis": vendor_basis(endpoint),
    }
    FigureStatement.model_validate(statement)
    return statement


def computed_statement(
    *,
    operation: str,
    inputs: Sequence[Mapping[str, Any]],
    period: Mapping[str, Any],
    parameters: Mapping[str, str | int | float | bool] | None = None,
    basis: Basis | Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Validated ``computed`` ``FigureStatement`` wire dict."""

    statement: dict[str, Any] = {
        "measure": {
            "kind": "computed",
            "operation": operation,
            "parameters": dict(parameters or {}),
            "inputs": [dict(item) for item in inputs],
        },
        "period": dict(period),
        "basis": dict(BASIS_NOT_RECORDED) if basis is None else (dict(basis) if isinstance(basis, Mapping) else basis),
    }
    FigureStatement.model_validate(statement)
    return statement


def _consecutive_calendar_quarter_dates(rows: Sequence[Mapping[str, Any]]) -> list[date] | None:
    dates: list[date] = []
    for row in rows:
        try:
            parsed = fmp_record_period_end(row)
        except ValueError:
            return None
        if parsed is None:
            return None
        dates.append(parsed)
    if len(set(dates)) != 4:
        return None
    ordered = sorted(dates)
    for earlier, later in zip(ordered, ordered[1:]):
        gap = (later - earlier).days
        if gap < 60 or gap > 120:
            return None
    return ordered


def trailing_four_quarter_sum(
    rows: Sequence[Mapping[str, Any]],
    *,
    endpoint: str,
    field: str,
    observed_at: datetime,
    parse_number: Callable[[Any], float | None] | None = None,
) -> tuple[float, dict[str, Any]] | None:
    """Sum the latest four consecutive quarterly rows and state the exact quarters summed.

    Returns ``None`` when fewer than 4 rows exist, any of the latest 4 rows lacks
    a finite number for ``field``, or the 4 rows are not consecutive quarters.
    """

    candidates = [row for row in rows if isinstance(row, Mapping)][:4]
    if len(candidates) < 4:
        return None

    values: list[float] = []
    for row in candidates:
        raw_val = row.get(field)
        if isinstance(raw_val, bool) or raw_val is None:
            return None
        if parse_number is not None:
            numeric = parse_number(raw_val)
        else:
            try:
                numeric = float(raw_val)
            except (TypeError, ValueError):
                numeric = None
        if numeric is None or not math.isfinite(numeric):
            return None
        values.append(numeric)

    member_periods: list[dict[str, Any]] = []
    explicit_non_quarter = False
    for row in candidates:
        raw_p = row.get("period")
        if raw_p is not None and not (isinstance(raw_p, float) and math.isnan(raw_p)):
            p_text = str(raw_p).strip().upper()
            if p_text and p_text not in ("Q1", "Q2", "Q3", "Q4"):
                explicit_non_quarter = True
                break
        member_periods.append(
            row_period(endpoint, {"period": "quarter"}, row, observed_at=observed_at)
        )
    if explicit_non_quarter:
        return None

    if all(period.get("kind") == "fiscal_period" for period in member_periods):
        parsed_members = [FiscalPeriod.model_validate(period) for period in member_periods]
        # Order chronologically to identify the latest period end deterministically.
        if all(
            isinstance(m.fiscal_year, int) and isinstance(m.quarter, int)
            for m in parsed_members
        ):
            latest_member = max(
                parsed_members,
                key=lambda m: (int(m.fiscal_year), int(m.quarter)),  # type: ignore[arg-type]
            )
            end_date_val: Any = (
                latest_member.end_date.model_dump(mode="json")
                if isinstance(latest_member.end_date, NotStated)
                else latest_member.end_date
            )
        else:
            ordered_dates = _consecutive_calendar_quarter_dates(candidates)
            if ordered_dates is None:
                return None
            end_date_val = ordered_dates[-1].isoformat()
        status = (
            "projected"
            if any(m.status == "projected" for m in parsed_members)
            else "actual"
        )
        trailing_dict: dict[str, Any] = {
            "kind": "trailing",
            "end_date": end_date_val,
            "status": status,
            "periods_summed": member_periods,
        }
        try:
            parsed_trailing = TrailingPeriod.model_validate(trailing_dict)
        except ValidationError:
            return None
        alignment = trailing_alignment(parsed_trailing)
        if alignment == "not_four_consecutive_quarters_ending_at_end_date":
            return None
        if alignment in ("end_date_not_recorded", "period_identity_not_recorded"):
            if all(
                isinstance(m.fiscal_year, int) and isinstance(m.quarter, int)
                for m in parsed_members
            ):
                indices = sorted(
                    int(m.fiscal_year) * 4 + int(m.quarter) - 1  # type: ignore[arg-type]
                    for m in parsed_members
                )
                if [b - a for a, b in zip(indices, indices[1:])] != [1, 1, 1]:
                    return None
            elif _consecutive_calendar_quarter_dates(candidates) is None:
                return None
    elif all(period == _ns("period_not_recorded") for period in member_periods):
        if any(row.get("date") is not None for row in candidates):
            ordered_dates = _consecutive_calendar_quarter_dates(candidates)
            if ordered_dates is None:
                return None
            end_date_slot: Any = ordered_dates[-1].isoformat()
        else:
            end_date_slot = _ns("end_date_not_recorded")
        trailing_dict = {
            "kind": "trailing",
            "end_date": end_date_slot,
            "status": "actual",
            "periods_summed": _ns("periods_not_recorded"),
        }
    else:
        return None

    v_measure = vendor_measure(endpoint, field, observed_at=observed_at)
    v_basis = vendor_basis(endpoint)
    statement = computed_statement(
        operation="sum",
        parameters={"quarters": 4},
        inputs=[
            {
                "kind": "statement",
                "measure": v_measure,
                "period": period,
                "basis": v_basis,
            }
            for period in member_periods
        ],
        period=trailing_dict,
    )
    return sum(values), statement


def vendor_row_statements(
    endpoint: str,
    params: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    *,
    observed_at: datetime,
    fields: Sequence[str] | None = None,
    place: Place = ("data",),
    first_index: int = 0,
) -> dict[Place, dict[str, Any]]:
    """Every figure of one response by its place in the answer, each with its statement."""

    presented = None if fields is None else frozenset(fields)
    measures: dict[str, dict[str, Any]] = {}
    basis = vendor_basis(endpoint)
    by_place: dict[Place, dict[str, Any]] = {}
    for index, row in enumerate(_normalized_response(rows), start=first_index):
        figure_fields = [
            field
            for field, value in row.items()
            if (presented is None or field in presented) and _is_figure(field, value)
        ]
        if not figure_fields:
            continue
        period = row_period(endpoint, params, row, observed_at=observed_at)
        for field in figure_fields:
            if field not in measures:
                measures[field] = vendor_measure(endpoint, field, observed_at=observed_at)
            by_place[(*place, index, field)] = {
                "measure": measures[field],
                "period": period,
                "basis": dict(basis) if isinstance(basis, Mapping) else basis,
            }
    return by_place


def _is_figure(field: str, value: Any) -> bool:
    if field in _PERIOD_IDENTITY_FIELDS:
        return False
    canonical = _canonical_scalar(value)
    return canonical is not None and canonical[0] in ("integer", "decimal")


def statements_table(
    by_place: Mapping[Place, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """The ``statements`` member: each figure's statement, said once where figures share it."""

    entries: dict[str, dict[str, Any]] = {}
    members: dict[Place, dict[tuple[int, Place], Mapping[str, Any]]] = {}
    for place, statement in by_place.items():
        row_at = next(
            (position for position, segment in enumerate(place) if type(segment) is int),
            None,
        )
        if row_at is None:
            _entry(entries, list(place)).update(
                {
                    fact: (dict(stated) if isinstance(stated, Mapping) else stated)
                    for fact, stated in statement.items()
                }
            )
        else:
            cells = members.setdefault(place[:row_at], {})
            cells[(place[row_at], place[row_at + 1 :])] = statement
    for member, cells in members.items():
        _state_member(entries, member, cells)
    table = sorted(
        entries.values(),
        key=lambda entry: (
            len(entry["at"]),
            ["" if segment is None else json.dumps(segment) for segment in entry["at"]],
        ),
    )
    if table:
        validate_statement_entries(table)
    return table


def _entry(entries: dict[str, dict[str, Any]], at: list[Any]) -> dict[str, Any]:
    return entries.setdefault(json.dumps(at), {"at": at})


def _state_member(
    entries: dict[str, dict[str, Any]],
    member: Place,
    cells: Mapping[tuple[int, Place], Mapping[str, Any]],
) -> None:
    rows_of_member = frozenset(row for row, _rest in cells)
    cells_of_row: dict[int, list[tuple[int, Place]]] = {}
    rows_of_column: dict[Place, set[int]] = {}
    for cell in cells:
        cells_of_row.setdefault(cell[0], []).append(cell)
        rows_of_column.setdefault(cell[1], set()).add(cell[0])

    stated: dict[tuple[str, str | None], dict[str, set[tuple[int, Place]]]] = {}
    values: dict[str, Any] = {}
    for cell, statement in cells.items():
        for fact, body in statement.items():
            whole = not isinstance(body, Mapping) or body.get("kind") == "not_stated"
            items = (
                ((None, dict(body) if isinstance(body, Mapping) else body),)
                if whole
                else body.items()
            )
            for key, value in items:
                token = json.dumps(value, sort_keys=True, separators=(",", ":"))
                values[token] = value
                stated.setdefault((fact, key), {}).setdefault(token, set()).add(cell)

    def put(
        rows: frozenset[int],
        every: bool,
        rest: Place,
        fact: str,
        key: str | None,
        token: str,
    ) -> None:
        index = None if every else min(rows) if len(rows) == 1 else sorted(rows)
        entry = _entry(entries, [*member, index, *rest])
        if key is None:
            entry[fact] = values[token]
        else:
            entry.setdefault(fact, {})[key] = values[token]

    for (fact, key), by_value in stated.items():
        for token, holders in by_value.items():
            whole_rows = frozenset(
                row
                for row, row_cells in cells_of_row.items()
                if all(cell in holders for cell in row_cells)
            )
            if whole_rows:
                put(whole_rows, whole_rows == rows_of_member, (), fact, key, token)
            by_column: dict[Place, set[int]] = {}
            for row, rest in holders:
                if row not in whole_rows:
                    by_column.setdefault(rest, set()).add(row)
            for rest, rows in by_column.items():
                put(frozenset(rows), rows == rows_of_column[rest], rest, fact, key, token)


__all__ = [
    "BASIS_NOT_RECORDED",
    "Basis",
    "FMP_FIELD_CLASSIFICATIONS",
    "FigureStatement",
    "FiscalPeriod",
    "NotStated",
    "NotStatedReason",
    "PeriodRule",
    "Place",
    "StatementEntry",
    "TrailingPeriod",
    "VENDOR_FIELD_CLASSIFICATIONS",
    "VENDOR_UNCOMPARED_BASIS",
    "VendorFieldClassification",
    "computed_statement",
    "expand_place_key",
    "expand_same_as_figure",
    "fmp_field_classification",
    "fmp_record_fiscal_year",
    "fmp_record_period_end",
    "fmp_row_currency",
    "not_stated",
    "parse_statements",
    "render_statement_label",
    "render_statements",
    "resolve_statements",
    "row_period",
    "statements_table",
    "trailing_alignment",
    "trailing_four_quarter_sum",
    "validate_statement_entries",
    "vendor_basis",
    "vendor_figure_statement",
    "vendor_measure",
    "vendor_row_statements",
]
