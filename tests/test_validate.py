"""Tests for the data validation framework."""

from __future__ import annotations

import pytest

from crochet.errors import ValidationError
from crochet.ingest.validate import (
    ColumnRule,
    DataSchema,
    Severity,
    ValidationIssue,
    ValidationResult,
    validate,
)


# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

PEOPLE = [
    {"id": 1, "name": "Alice", "age": 30, "email": "alice@example.com"},
    {"id": 2, "name": "Bob", "age": 25, "email": "bob@example.com"},
    {"id": 3, "name": "Charlie", "age": 35, "email": "charlie@example.com"},
]


# ---------------------------------------------------------------------------
# ValidationIssue / ValidationResult
# ---------------------------------------------------------------------------


class TestValidationIssue:
    def test_str_with_row_and_column(self):
        issue = ValidationIssue(row=0, column="name", message="bad value")
        s = str(issue)
        assert "row 0" in s
        assert "column 'name'" in s
        assert "bad value" in s
        assert "[ERROR]" in s

    def test_str_warning(self):
        issue = ValidationIssue(
            row=None, column="extra", message="unexpected",
            severity=Severity.WARNING,
        )
        assert "[WARNING]" in str(issue)

    def test_str_no_location(self):
        issue = ValidationIssue(row=None, column=None, message="global issue")
        assert "global issue" in str(issue)


class TestValidationResult:
    def test_empty_is_valid(self):
        result = ValidationResult()
        assert result.is_valid
        assert result.error_count == 0
        assert result.warning_count == 0

    def test_errors_separate_from_warnings(self):
        result = ValidationResult(issues=[
            ValidationIssue(row=0, column="a", message="err"),
            ValidationIssue(row=1, column="b", message="warn", severity=Severity.WARNING),
        ])
        assert not result.is_valid
        assert result.error_count == 1
        assert result.warning_count == 1

    def test_warnings_only_is_valid(self):
        result = ValidationResult(issues=[
            ValidationIssue(row=0, column="a", message="warn", severity=Severity.WARNING),
        ])
        assert result.is_valid

    def test_summary_passed(self):
        result = ValidationResult()
        assert "passed" in result.summary()

    def test_summary_failed(self):
        result = ValidationResult(issues=[
            ValidationIssue(row=0, column="a", message="missing"),
        ])
        assert "failed" in result.summary()

    def test_raise_on_errors(self):
        result = ValidationResult(issues=[
            ValidationIssue(row=0, column="a", message="bad"),
        ])
        with pytest.raises(ValidationError):
            result.raise_on_errors()

    def test_raise_on_no_errors(self):
        result = ValidationResult()
        result.raise_on_errors()  # should not raise

    def test_summary_truncation(self):
        issues = [
            ValidationIssue(row=i, column="x", message=f"err {i}")
            for i in range(30)
        ]
        result = ValidationResult(issues=issues)
        summary = result.summary()
        assert "more issue(s)" in summary


# ---------------------------------------------------------------------------
# Required fields
# ---------------------------------------------------------------------------


class TestRequired:
    def test_all_present(self):
        schema = DataSchema()
        schema.column("name", required=True)
        schema.column("age", required=True)
        result = validate(PEOPLE, schema)
        assert result.is_valid

    def test_missing_value(self):
        data = [{"id": 1, "name": None, "age": 30}]
        schema = DataSchema()
        schema.column("name", required=True)
        result = validate(data, schema)
        assert not result.is_valid
        assert result.error_count == 1
        assert "Required" in result.errors[0].message

    def test_missing_key(self):
        data = [{"id": 1, "age": 30}]
        schema = DataSchema()
        schema.column("name", required=True)
        result = validate(data, schema)
        assert not result.is_valid


# ---------------------------------------------------------------------------
# Type checks
# ---------------------------------------------------------------------------


class TestTypeCheck:
    def test_correct_types(self):
        schema = DataSchema()
        schema.column("name", dtype="str")
        schema.column("age", dtype="int")
        result = validate(PEOPLE, schema)
        assert result.is_valid

    def test_wrong_type(self):
        data = [{"name": 123}]
        schema = DataSchema()
        schema.column("name", dtype="str")
        result = validate(data, schema)
        assert not result.is_valid
        assert "Expected type" in result.errors[0].message

    def test_float_accepts_int(self):
        data = [{"value": 42}]
        schema = DataSchema()
        schema.column("value", dtype="float")
        result = validate(data, schema)
        assert result.is_valid

    def test_null_skips_type_check(self):
        data = [{"name": None}]
        schema = DataSchema()
        schema.column("name", dtype="str")  # not required
        result = validate(data, schema)
        assert result.is_valid


# ---------------------------------------------------------------------------
# Numeric range
# ---------------------------------------------------------------------------


class TestNumericRange:
    def test_in_range(self):
        schema = DataSchema()
        schema.column("age", min_value=0, max_value=150)
        result = validate(PEOPLE, schema)
        assert result.is_valid

    def test_below_min(self):
        data = [{"age": -5}]
        schema = DataSchema()
        schema.column("age", min_value=0)
        result = validate(data, schema)
        assert not result.is_valid
        assert "minimum" in result.errors[0].message

    def test_above_max(self):
        data = [{"age": 200}]
        schema = DataSchema()
        schema.column("age", max_value=150)
        result = validate(data, schema)
        assert not result.is_valid
        assert "maximum" in result.errors[0].message


# ---------------------------------------------------------------------------
# String length
# ---------------------------------------------------------------------------


class TestStringLength:
    def test_in_range(self):
        schema = DataSchema()
        schema.column("name", min_length=1, max_length=100)
        result = validate(PEOPLE, schema)
        assert result.is_valid

    def test_too_short(self):
        data = [{"name": ""}]
        schema = DataSchema()
        schema.column("name", min_length=1)
        result = validate(data, schema)
        assert not result.is_valid
        assert "minimum" in result.errors[0].message

    def test_too_long(self):
        data = [{"name": "A" * 101}]
        schema = DataSchema()
        schema.column("name", max_length=100)
        result = validate(data, schema)
        assert not result.is_valid
        assert "maximum" in result.errors[0].message


# ---------------------------------------------------------------------------
# Pattern
# ---------------------------------------------------------------------------


class TestPattern:
    def test_match(self):
        schema = DataSchema()
        schema.column("email", pattern=r".+@.+\..+")
        result = validate(PEOPLE, schema)
        assert result.is_valid

    def test_no_match(self):
        data = [{"email": "not-an-email"}]
        schema = DataSchema()
        schema.column("email", pattern=r".+@.+\..+")
        result = validate(data, schema)
        assert not result.is_valid
        assert "pattern" in result.errors[0].message


# ---------------------------------------------------------------------------
# Allowed values
# ---------------------------------------------------------------------------


class TestAllowed:
    def test_valid(self):
        data = [{"status": "active"}, {"status": "inactive"}]
        schema = DataSchema()
        schema.column("status", allowed={"active", "inactive", "pending"})
        result = validate(data, schema)
        assert result.is_valid

    def test_invalid(self):
        data = [{"status": "deleted"}]
        schema = DataSchema()
        schema.column("status", allowed={"active", "inactive"})
        result = validate(data, schema)
        assert not result.is_valid
        assert "allowed" in result.errors[0].message


# ---------------------------------------------------------------------------
# Custom validators
# ---------------------------------------------------------------------------


class TestCustomValidator:
    def test_passing(self):
        schema = DataSchema()
        schema.column("age", custom=lambda v: None if v > 0 else "must be positive")
        result = validate(PEOPLE, schema)
        assert result.is_valid

    def test_failing(self):
        data = [{"age": -1}]
        schema = DataSchema()
        schema.column("age", custom=lambda v: None if v > 0 else "must be positive")
        result = validate(data, schema)
        assert not result.is_valid
        assert "must be positive" in result.errors[0].message


# ---------------------------------------------------------------------------
# Uniqueness
# ---------------------------------------------------------------------------


class TestUniqueness:
    def test_unique(self):
        schema = DataSchema(unique_columns=["id"])
        result = validate(PEOPLE, schema)
        assert result.is_valid

    def test_duplicate(self):
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 1, "name": "Alice2"},
        ]
        schema = DataSchema(unique_columns=["id"])
        result = validate(data, schema)
        assert not result.is_valid
        assert "Duplicate" in result.errors[0].message
        assert "first seen at row 0" in result.errors[0].message


# ---------------------------------------------------------------------------
# Row count
# ---------------------------------------------------------------------------


class TestRowCount:
    def test_min_rows(self):
        schema = DataSchema(min_rows=5)
        result = validate(PEOPLE, schema)  # 3 rows < 5
        assert not result.is_valid
        assert "at least" in result.errors[0].message

    def test_max_rows(self):
        schema = DataSchema(max_rows=2)
        result = validate(PEOPLE, schema)  # 3 rows > 2
        assert not result.is_valid
        assert "at most" in result.errors[0].message

    def test_within_bounds(self):
        schema = DataSchema(min_rows=1, max_rows=10)
        result = validate(PEOPLE, schema)
        assert result.is_valid


# ---------------------------------------------------------------------------
# Strict mode
# ---------------------------------------------------------------------------


class TestStrictMode:
    def test_extra_columns_warn(self):
        schema = DataSchema(strict=True)
        schema.column("id")
        schema.column("name")
        # "age" and "email" are extra
        result = validate(PEOPLE, schema)
        assert result.is_valid  # warnings don't fail
        assert result.warning_count >= 2

    def test_no_extra_columns(self):
        data = [{"id": 1, "name": "Alice"}]
        schema = DataSchema(strict=True)
        schema.column("id")
        schema.column("name")
        result = validate(data, schema)
        assert result.warning_count == 0


# ---------------------------------------------------------------------------
# Chaining
# ---------------------------------------------------------------------------


class TestChaining:
    def test_fluent_api(self):
        schema = (
            DataSchema(unique_columns=["id"])
            .column("id", required=True, dtype="int")
            .column("name", required=True, dtype="str", min_length=1)
            .column("age", dtype="int", min_value=0, max_value=150)
            .column("email", pattern=r".+@.+\..+")
        )
        result = validate(PEOPLE, schema)
        assert result.is_valid
        assert len(schema.columns) == 4


# ---------------------------------------------------------------------------
# Severity override
# ---------------------------------------------------------------------------


class TestSeverityOverride:
    def test_warning_severity(self):
        data = [{"age": -1}]
        schema = DataSchema()
        schema.column("age", min_value=0, severity=Severity.WARNING)
        result = validate(data, schema)
        assert result.is_valid  # warnings don't fail
        assert result.warning_count == 1
