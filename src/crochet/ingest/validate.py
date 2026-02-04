"""Data validation framework for pre-flight checks before loading into Neo4j.

Validates records (list[dict]) against a declarative schema of column rules.
Composable validators cover required fields, type checking, value ranges,
regex patterns, allowed values, and custom predicates.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable


class Severity(Enum):
    ERROR = "error"
    WARNING = "warning"


@dataclass(frozen=True)
class ValidationIssue:
    """A single validation problem."""

    row: int | None
    column: str | None
    message: str
    severity: Severity = Severity.ERROR

    def __str__(self) -> str:
        loc = ""
        if self.row is not None:
            loc += f"row {self.row}"
        if self.column:
            loc += f", column '{self.column}'" if loc else f"column '{self.column}'"
        prefix = f"[{self.severity.value.upper()}]"
        if loc:
            return f"{prefix} {loc}: {self.message}"
        return f"{prefix} {self.message}"


@dataclass
class ValidationResult:
    """Aggregated result of validating a dataset."""

    issues: list[ValidationIssue] = field(default_factory=list)

    @property
    def errors(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == Severity.ERROR]

    @property
    def warnings(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == Severity.WARNING]

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0

    @property
    def error_count(self) -> int:
        return len(self.errors)

    @property
    def warning_count(self) -> int:
        return len(self.warnings)

    def summary(self) -> str:
        lines = []
        if self.is_valid:
            lines.append("Validation passed.")
        else:
            lines.append(f"Validation failed with {self.error_count} error(s).")
        if self.warnings:
            lines.append(f"  {self.warning_count} warning(s).")
        for issue in self.issues[:20]:
            lines.append(f"  {issue}")
        if len(self.issues) > 20:
            lines.append(f"  … and {len(self.issues) - 20} more issue(s).")
        return "\n".join(lines)

    def raise_on_errors(self) -> None:
        """Raise ``ValidationError`` if there are any errors."""
        if not self.is_valid:
            from crochet.errors import ValidationError

            raise ValidationError(self)


# ---------------------------------------------------------------------------
# Column rules
# ---------------------------------------------------------------------------

_TYPE_MAP: dict[str, type | tuple[type, ...]] = {
    "str": (str,),
    "string": (str,),
    "int": (int,),
    "integer": (int,),
    "float": (float, int),
    "number": (float, int),
    "bool": (bool,),
    "boolean": (bool,),
}


@dataclass
class ColumnRule:
    """Declarative validation rule for a single column.

    Parameters
    ----------
    name:
        Column name.
    required:
        If ``True``, the column must exist and be non-null in every row.
    dtype:
        Expected Python type name (``"str"``, ``"int"``, ``"float"``,
        ``"bool"``).  Checked only for non-null values.
    min_value:
        Minimum numeric value (inclusive).
    max_value:
        Maximum numeric value (inclusive).
    min_length:
        Minimum string length.
    max_length:
        Maximum string length.
    pattern:
        Regex pattern the value must match (strings only).
    allowed:
        Set of allowed values.
    custom:
        Callable ``(value) -> str | None``.  Return an error message string
        on failure, or ``None`` on success.
    severity:
        Default severity for issues raised by this rule.
    """

    name: str
    required: bool = False
    dtype: str | None = None
    min_value: float | int | None = None
    max_value: float | int | None = None
    min_length: int | None = None
    max_length: int | None = None
    pattern: str | None = None
    allowed: set[Any] | list[Any] | None = None
    custom: Callable[[Any], str | None] | None = None
    severity: Severity = Severity.ERROR

    def _compiled_pattern(self) -> re.Pattern | None:
        if self.pattern is None:
            return None
        return re.compile(self.pattern)


@dataclass
class DataSchema:
    """Schema describing expected columns and dataset-level constraints.

    Parameters
    ----------
    columns:
        Column-level rules.
    strict:
        When ``True``, extra columns not listed in *columns* produce warnings.
    min_rows:
        Minimum expected row count.
    max_rows:
        Maximum expected row count.
    unique_columns:
        Columns whose values must be unique across all rows.
    """

    columns: list[ColumnRule] = field(default_factory=list)
    strict: bool = False
    min_rows: int | None = None
    max_rows: int | None = None
    unique_columns: list[str] = field(default_factory=list)

    def column(
        self,
        name: str,
        *,
        required: bool = False,
        dtype: str | None = None,
        min_value: float | int | None = None,
        max_value: float | int | None = None,
        min_length: int | None = None,
        max_length: int | None = None,
        pattern: str | None = None,
        allowed: set[Any] | list[Any] | None = None,
        custom: Callable[[Any], str | None] | None = None,
        severity: Severity = Severity.ERROR,
    ) -> "DataSchema":
        """Add a column rule and return ``self`` for chaining."""
        self.columns.append(
            ColumnRule(
                name=name,
                required=required,
                dtype=dtype,
                min_value=min_value,
                max_value=max_value,
                min_length=min_length,
                max_length=max_length,
                pattern=pattern,
                allowed=allowed,
                custom=custom,
                severity=severity,
            )
        )
        return self


# ---------------------------------------------------------------------------
# Validation engine
# ---------------------------------------------------------------------------


def validate(
    records: list[dict[str, Any]],
    schema: DataSchema,
) -> ValidationResult:
    """Validate *records* against *schema*.

    Returns a `ValidationResult` containing all issues found.
    """
    result = ValidationResult()
    rules_by_name: dict[str, ColumnRule] = {r.name: r for r in schema.columns}
    declared_names = set(rules_by_name)

    # --- Row count checks ---
    if schema.min_rows is not None and len(records) < schema.min_rows:
        result.issues.append(
            ValidationIssue(
                row=None,
                column=None,
                message=f"Expected at least {schema.min_rows} rows, got {len(records)}.",
            )
        )
    if schema.max_rows is not None and len(records) > schema.max_rows:
        result.issues.append(
            ValidationIssue(
                row=None,
                column=None,
                message=f"Expected at most {schema.max_rows} rows, got {len(records)}.",
            )
        )

    # --- Strict mode: check for extra columns ---
    if schema.strict and records:
        all_cols: set[str] = set()
        for row in records:
            all_cols.update(row.keys())
        extra = all_cols - declared_names
        for col in sorted(extra):
            result.issues.append(
                ValidationIssue(
                    row=None,
                    column=col,
                    message="Unexpected column (strict mode).",
                    severity=Severity.WARNING,
                )
            )

    # --- Uniqueness checks ---
    unique_trackers: dict[str, dict[Any, int]] = {
        col: {} for col in schema.unique_columns
    }

    # --- Per-row validation ---
    for row_idx, row in enumerate(records):
        # Column-level rules
        for rule in schema.columns:
            value = row.get(rule.name)
            sev = rule.severity

            # Required check
            if rule.required and (rule.name not in row or value is None):
                result.issues.append(
                    ValidationIssue(
                        row=row_idx, column=rule.name,
                        message="Required value is missing.",
                        severity=sev,
                    )
                )
                continue  # skip further checks for this column

            # Skip null values for non-required columns
            if value is None:
                continue

            # Type check
            if rule.dtype:
                expected_types = _TYPE_MAP.get(rule.dtype.lower())
                if expected_types and not isinstance(value, expected_types):
                    result.issues.append(
                        ValidationIssue(
                            row=row_idx, column=rule.name,
                            message=(
                                f"Expected type '{rule.dtype}', "
                                f"got '{type(value).__name__}'."
                            ),
                            severity=sev,
                        )
                    )

            # Numeric range checks
            if rule.min_value is not None and isinstance(value, (int, float)):
                if value < rule.min_value:
                    result.issues.append(
                        ValidationIssue(
                            row=row_idx, column=rule.name,
                            message=f"Value {value} < minimum {rule.min_value}.",
                            severity=sev,
                        )
                    )
            if rule.max_value is not None and isinstance(value, (int, float)):
                if value > rule.max_value:
                    result.issues.append(
                        ValidationIssue(
                            row=row_idx, column=rule.name,
                            message=f"Value {value} > maximum {rule.max_value}.",
                            severity=sev,
                        )
                    )

            # String length checks
            if rule.min_length is not None and isinstance(value, str):
                if len(value) < rule.min_length:
                    result.issues.append(
                        ValidationIssue(
                            row=row_idx, column=rule.name,
                            message=(
                                f"Length {len(value)} < minimum {rule.min_length}."
                            ),
                            severity=sev,
                        )
                    )
            if rule.max_length is not None and isinstance(value, str):
                if len(value) > rule.max_length:
                    result.issues.append(
                        ValidationIssue(
                            row=row_idx, column=rule.name,
                            message=(
                                f"Length {len(value)} > maximum {rule.max_length}."
                            ),
                            severity=sev,
                        )
                    )

            # Pattern check
            if rule.pattern and isinstance(value, str):
                compiled = rule._compiled_pattern()
                if compiled and not compiled.fullmatch(value):
                    result.issues.append(
                        ValidationIssue(
                            row=row_idx, column=rule.name,
                            message=f"Value '{value}' does not match pattern '{rule.pattern}'.",
                            severity=sev,
                        )
                    )

            # Allowed values
            if rule.allowed is not None:
                if value not in rule.allowed:
                    result.issues.append(
                        ValidationIssue(
                            row=row_idx, column=rule.name,
                            message=f"Value '{value}' not in allowed set.",
                            severity=sev,
                        )
                    )

            # Custom validator
            if rule.custom is not None:
                err = rule.custom(value)
                if err:
                    result.issues.append(
                        ValidationIssue(
                            row=row_idx, column=rule.name,
                            message=err,
                            severity=sev,
                        )
                    )

        # Uniqueness tracking
        for col_name in schema.unique_columns:
            val = row.get(col_name)
            if val is None:
                continue
            tracker = unique_trackers[col_name]
            if val in tracker:
                result.issues.append(
                    ValidationIssue(
                        row=row_idx, column=col_name,
                        message=(
                            f"Duplicate value '{val}' "
                            f"(first seen at row {tracker[val]})."
                        ),
                    )
                )
            else:
                tracker[val] = row_idx

    return result
