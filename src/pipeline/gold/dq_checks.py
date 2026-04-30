# Databricks notebook source
# src/pipeline/gold/dq_checks.py
"""
Post-pipeline data quality gate.
Fails the job task (raises) if any critical check fails.
Non-critical checks are logged as warnings.
"""

# COMMAND ----------
import logging
from dataclasses import dataclass, field
from typing import Callable

from pyspark.sql import DataFrame, functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── Parameters ────────────────────────────────────────────────────────────────
dbutils.widgets.text("catalog_name", "medallion_dev", "Catalog")
dbutils.widgets.text("env",          "dev",           "Environment")

CATALOG = dbutils.widgets.get("catalog_name")
ENV     = dbutils.widgets.get("env")

# ── DQ framework ──────────────────────────────────────────────────────────────

@dataclass
class DQResult:
    check_name:   str
    table:        str
    passed:       bool
    actual_value: float | None = None
    threshold:    float | None = None
    severity:     str = "CRITICAL"   # CRITICAL | WARNING
    message:      str = ""


def run_checks(checks: list[Callable[[], DQResult]]) -> None:
    results: list[DQResult] = [c() for c in checks]
    failures = [r for r in results if not r.passed and r.severity == "CRITICAL"]
    warnings = [r for r in results if not r.passed and r.severity == "WARNING"]

    for r in results:
        level = "✅" if r.passed else ("❌" if r.severity == "CRITICAL" else "⚠️")
        log.info("%s  [%s] %s | %s", level, r.table, r.check_name, r.message)

    if warnings:
        log.warning("DQ warnings (%d): %s", len(warnings), [w.check_name for w in warnings])
    if failures:
        raise RuntimeError(
            f"DQ CRITICAL failures ({len(failures)}): "
            + ", ".join(f"{f.check_name} on {f.table}" for f in failures)
        )

# ─────────────────────────────────────────────────────────────────────────────
# Check definitions
# ─────────────────────────────────────────────────────────────────────────────

def check_gold_daily_sales_not_empty() -> DQResult:
    count = spark.table(f"{CATALOG}.dlt_schema.gold_daily_sales").count()
    return DQResult(
        "not_empty", "gold_daily_sales",
        passed=count > 0,
        actual_value=count,
        message=f"row count={count}"
    )


def check_gold_daily_sales_no_null_revenue() -> DQResult:
    df = spark.table(f"{CATALOG}.dlt_schema.gold_daily_sales")
    null_count = df.filter(F.col("gross_revenue").isNull()).count()
    return DQResult(
        "no_null_revenue", "gold_daily_sales",
        passed=null_count == 0,
        actual_value=null_count,
        message=f"null gross_revenue rows={null_count}"
    )


def check_gold_daily_sales_revenue_positive() -> DQResult:
    df = spark.table(f"{CATALOG}.dlt_schema.gold_daily_sales")
    negative = df.filter(F.col("gross_revenue") < 0).count()
    return DQResult(
        "revenue_positive", "gold_daily_sales",
        passed=negative == 0,
        actual_value=negative,
        message=f"negative revenue rows={negative}"
    )


def check_customer_clv_not_empty() -> DQResult:
    count = spark.table(f"{CATALOG}.dlt_schema.gold_customer_clv").count()
    return DQResult(
        "not_empty", "gold_customer_clv",
        passed=count > 0,
        actual_value=count,
        message=f"row count={count}"
    )


def check_clv_segments_populated() -> DQResult:
    df = spark.table(f"{CATALOG}.dlt_schema.gold_customer_clv")
    null_segs = df.filter(F.col("clv_segment").isNull()).count()
    return DQResult(
        "clv_segment_not_null", "gold_customer_clv",
        passed=null_segs == 0,
        actual_value=null_segs,
        severity="WARNING",
        message=f"null clv_segment rows={null_segs}"
    )


def check_product_performance_not_empty() -> DQResult:
    count = spark.table(f"{CATALOG}.dlt_schema.gold_product_performance").count()
    return DQResult(
        "not_empty", "gold_product_performance",
        passed=count > 0,
        actual_value=count,
        message=f"row count={count}"
    )


def check_regional_kpis_not_empty() -> DQResult:
    count = spark.table(f"{CATALOG}.dlt_schema.gold_regional_kpis").count()
    return DQResult(
        "not_empty", "gold_regional_kpis",
        passed=count > 0,
        actual_value=count,
        message=f"row count={count}"
    )


# COMMAND ----------
log.info("=== DQ Checks START (env=%s, catalog=%s) ===", ENV, CATALOG)

run_checks([
    check_gold_daily_sales_not_empty,
    check_gold_daily_sales_no_null_revenue,
    check_gold_daily_sales_revenue_positive,
    check_customer_clv_not_empty,
    check_clv_segments_populated,
    check_product_performance_not_empty,
    check_regional_kpis_not_empty,
])

log.info("=== DQ Checks PASSED ===")
