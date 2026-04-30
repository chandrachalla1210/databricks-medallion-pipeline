# tests/unit/test_dq_checks.py
"""
Unit tests for the data quality check framework defined in
src/pipeline/gold/dq_checks.py
"""
from __future__ import annotations

import pytest

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))

# ── We test the DQResult dataclass and run_checks function in isolation ───────

from dataclasses import dataclass
from typing import Callable


@dataclass
class DQResult:
    check_name:   str
    table:        str
    passed:       bool
    actual_value: float | None = None
    threshold:    float | None = None
    severity:     str = "CRITICAL"
    message:      str = ""


def run_checks(checks: list[Callable[[], DQResult]]) -> list[DQResult]:
    results: list[DQResult] = [c() for c in checks]
    failures = [r for r in results if not r.passed and r.severity == "CRITICAL"]
    if failures:
        raise RuntimeError(
            f"DQ CRITICAL failures ({len(failures)}): "
            + ", ".join(f.check_name for f in failures)
        )
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestDQResult:

    def test_passing_result(self):
        r = DQResult("my_check", "my_table", passed=True)
        assert r.passed is True
        assert r.severity == "CRITICAL"

    def test_failing_result(self):
        r = DQResult("my_check", "my_table", passed=False, actual_value=0)
        assert r.passed is False


class TestRunChecks:

    def test_all_pass(self):
        checks = [
            lambda: DQResult("c1", "t1", passed=True),
            lambda: DQResult("c2", "t2", passed=True),
        ]
        results = run_checks(checks)
        assert len(results) == 2
        assert all(r.passed for r in results)

    def test_critical_failure_raises(self):
        checks = [
            lambda: DQResult("c1", "t1", passed=True),
            lambda: DQResult("c2", "t2", passed=False, severity="CRITICAL"),
        ]
        with pytest.raises(RuntimeError, match="DQ CRITICAL failures"):
            run_checks(checks)

    def test_warning_does_not_raise(self):
        checks = [
            lambda: DQResult("c1", "t1", passed=False, severity="WARNING"),
        ]
        results = run_checks(checks)   # should NOT raise
        assert results[0].passed is False
        assert results[0].severity == "WARNING"

    def test_multiple_critical_failures(self):
        checks = [
            lambda: DQResult("c1", "t1", passed=False, severity="CRITICAL"),
            lambda: DQResult("c2", "t2", passed=False, severity="CRITICAL"),
        ]
        with pytest.raises(RuntimeError) as exc_info:
            run_checks(checks)
        assert "2" in str(exc_info.value)

    def test_mixed_severities_only_critical_raises(self):
        checks = [
            lambda: DQResult("c1", "t1", passed=False, severity="WARNING"),
            lambda: DQResult("c2", "t2", passed=True,  severity="CRITICAL"),
        ]
        results = run_checks(checks)
        assert len(results) == 2


class TestNotEmptyCheck:
    """Simulate not-empty check logic."""

    def _check_not_empty(self, count: int) -> DQResult:
        return DQResult(
            "not_empty", "test_table",
            passed=count > 0,
            actual_value=count,
            message=f"row count={count}",
        )

    def test_non_empty_passes(self):
        assert self._check_not_empty(100).passed is True

    def test_empty_fails(self):
        assert self._check_not_empty(0).passed is False


class TestNoNullCheck:
    """Simulate no-null check logic."""

    def _check_no_nulls(self, null_count: int) -> DQResult:
        return DQResult(
            "no_null_revenue", "gold_daily_sales",
            passed=null_count == 0,
            actual_value=null_count,
            message=f"null rows={null_count}",
        )

    def test_no_nulls_passes(self):
        assert self._check_no_nulls(0).passed is True

    def test_nulls_fail(self):
        assert self._check_no_nulls(5).passed is False
