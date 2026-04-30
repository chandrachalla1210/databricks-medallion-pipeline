"""Unit tests for data quality check logic."""
import sys
import pytest
sys.path.insert(0, "src")
from pyspark.sql.types import *
from pyspark.sql import functions as F
from src.utils.helpers import compute_null_percentages

NULL_THRESHOLD_PCT = 5.0


def _run_quality_check(df, critical_columns, null_threshold=NULL_THRESHOLD_PCT):
    null_pcts = compute_null_percentages(df.select(critical_columns))
    failures = []
    for col, pct in null_pcts.items():
        if pct > null_threshold:
            failures.append(f"{col}: {pct}% nulls > threshold {null_threshold}%")
    return failures


class TestQualityCheckNullThresholds:

    def test_passes_with_no_nulls(self, spark):
        df = spark.createDataFrame(
            [("2024-01-01", 100.0, 5)],
            schema=StructType([
                StructField("order_date",    StringType()),
                StructField("total_revenue", DoubleType()),
                StructField("order_count",   IntegerType()),
            ])
        )
        failures = _run_quality_check(df, ["order_date", "total_revenue", "order_count"])
        assert failures == []

    def test_fails_with_high_null_rate(self, spark):
        data = [(None, 1.0)] * 10 + [("2024-01-01", 2.0)] * 0
        schema = StructType([
            StructField("order_date",    StringType()),
            StructField("total_revenue", DoubleType()),
        ])
        df = spark.createDataFrame(data, schema)
        failures = _run_quality_check(df, ["order_date"])
        assert len(failures) > 0
        assert "order_date" in failures[0]

    def test_passes_with_low_null_rate(self, spark):
        # 2% nulls — below 5% threshold
        data = [(None, 1.0)] * 2 + [("2024-01-01", 2.0)] * 98
        schema = StructType([
            StructField("order_date",    StringType()),
            StructField("total_revenue", DoubleType()),
        ])
        df = spark.createDataFrame(data, schema)
        failures = _run_quality_check(df, ["order_date"])
        assert failures == []


class TestRowCountChecks:

    def test_empty_table_fails(self, spark):
        schema = StructType([StructField("id", StringType())])
        df = spark.createDataFrame([], schema)
        assert df.count() < 1   # Minimum row count check

    def test_non_empty_table_passes(self, spark):
        df = spark.createDataFrame([("X",)], schema=StructType([StructField("id", StringType())]))
        assert df.count() >= 1
