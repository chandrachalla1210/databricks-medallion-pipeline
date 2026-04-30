# tests/unit/test_silver.py
"""
Unit tests for Silver layer transformation logic.
Validates type casting, deduplication, business rules, and enrichment.
"""
from __future__ import annotations

import pytest
from datetime import date
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, DateType, StringType

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))


# ─────────────────────────────────────────────────────────────────────────────
# Transformation helpers (mirror silver_pipeline.py logic for unit testing)
# ─────────────────────────────────────────────────────────────────────────────

def cast_order_types(df):
    return (
        df
        .withColumn("quantity",   F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
        .withColumn("order_date", F.to_date(F.col("order_date"), "yyyy-MM-dd"))
        .withColumn("order_year", F.year("order_date").cast(StringType()))
    )


def compute_line_total(df):
    return df.withColumn("line_total", F.round(F.col("quantity") * F.col("unit_price"), 2))


def normalise_strings(df):
    return (
        df
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        .withColumn("region", F.upper(F.trim(F.col("region"))))
    )


def deduplicate_orders(df):
    return df.dropDuplicates(["order_id"])


def filter_valid_statuses(df):
    VALID = ("pending", "processing", "shipped", "delivered", "cancelled", "returned")
    return df.filter(F.col("status").isin(VALID))


def enrich_orders_with_customer(orders_df, customers_df):
    return orders_df.join(
        customers_df.select("customer_id", "first_name", "last_name", "email", "country"),
        "customer_id", "left"
    )


def compute_discount_pct(orders_df, products_df):
    enriched = orders_df.join(
        products_df.select("product_id", "base_price"),
        "product_id", "left"
    )
    return enriched.withColumn(
        "discount_pct",
        F.when(
            F.col("unit_price") < F.col("base_price"),
            F.round((1 - F.col("unit_price") / F.col("base_price")) * 100, 2)
        ).otherwise(F.lit(0.0))
    )


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestTypeCasting:

    def test_quantity_cast_to_integer(self, raw_orders):
        good = raw_orders.filter(F.col("order_id").isNotNull() & F.col("quantity").isNotNull())
        result = cast_order_types(good)
        assert result.schema["quantity"].dataType == IntegerType()

    def test_unit_price_cast_to_double(self, raw_orders):
        good = raw_orders.filter(F.col("order_id").isNotNull())
        result = cast_order_types(good)
        assert result.schema["unit_price"].dataType == DoubleType()

    def test_order_date_parsed(self, raw_orders):
        good = raw_orders.filter(F.col("order_id").isNotNull())
        result = cast_order_types(good)
        assert result.schema["order_date"].dataType == DateType()

    def test_order_year_derived(self, raw_orders):
        good = raw_orders.filter(F.col("order_id").isNotNull())
        result = cast_order_types(good)
        years = {r["order_year"] for r in result.collect() if r["order_year"]}
        assert "2024" in years


class TestLineTotal:

    def test_line_total_correct(self, spark):
        from pyspark.sql.types import StructType, StructField
        rows = [("O1", 2, 19.99), ("O2", 5, 9.50)]
        df = spark.createDataFrame(rows, ["order_id", "quantity", "unit_price"])
        result = compute_line_total(df)
        totals = {r["order_id"]: r["line_total"] for r in result.collect()}
        assert totals["O1"] == pytest.approx(39.98, abs=0.01)
        assert totals["O2"] == pytest.approx(47.50, abs=0.01)

    def test_line_total_rounded_to_2dp(self, spark):
        rows = [("O1", 3, 3.333)]
        df = spark.createDataFrame(rows, ["order_id", "quantity", "unit_price"])
        result = compute_line_total(df)
        val = result.first()["line_total"]
        assert round(val, 2) == val


class TestStringNormalisation:

    def test_status_lowercased(self, spark):
        rows = [("O1", "DELIVERED", "EMEA"), ("O2", " Shipped ", "AMER")]
        df = spark.createDataFrame(rows, ["order_id", "status", "region"])
        result = normalise_strings(df)
        statuses = {r["status"] for r in result.collect()}
        assert "delivered" in statuses
        assert "shipped"   in statuses

    def test_region_uppercased_and_trimmed(self, spark):
        rows = [("O1", "delivered", " emea ")]
        df = spark.createDataFrame(rows, ["order_id", "status", "region"])
        result = normalise_strings(df)
        assert result.first()["region"] == "EMEA"


class TestDeduplication:

    def test_duplicates_removed(self, spark):
        rows = [("O1", "C1"), ("O1", "C1"), ("O2", "C2")]
        df = spark.createDataFrame(rows, ["order_id", "customer_id"])
        result = deduplicate_orders(df)
        assert result.count() == 2

    def test_unique_rows_preserved(self, spark):
        rows = [("O1", "C1"), ("O2", "C2"), ("O3", "C3")]
        df = spark.createDataFrame(rows, ["order_id", "customer_id"])
        result = deduplicate_orders(df)
        assert result.count() == 3


class TestStatusValidation:

    def test_valid_statuses_kept(self, silver_orders):
        result = filter_valid_statuses(silver_orders)
        assert result.count() == silver_orders.count()   # all statuses valid in fixture

    def test_invalid_status_filtered(self, spark):
        rows = [("O1", "delivered"), ("O2", "unknown_status"), ("O3", "shipped")]
        df = spark.createDataFrame(rows, ["order_id", "status"])
        result = filter_valid_statuses(df)
        assert result.count() == 2


class TestEnrichment:

    def test_customer_name_joined(self, spark, raw_customers):
        orders = spark.createDataFrame(
            [("O1", "C000001", "P001")], ["order_id", "customer_id", "product_id"]
        )
        result = enrich_orders_with_customer(orders, raw_customers)
        row = result.first()
        assert row["first_name"] == "Alice"
        assert row["country"]    == "US"

    def test_unmatched_customer_null(self, spark, raw_customers):
        orders = spark.createDataFrame(
            [("O1", "C999999", "P001")], ["order_id", "customer_id", "product_id"]
        )
        result = enrich_orders_with_customer(orders, raw_customers)
        assert result.count() == 1          # left join keeps order
        assert result.first()["first_name"] is None

    def test_discount_pct_computed(self, spark, raw_products):
        orders = spark.createDataFrame(
            [("O1", "C1", "P001", 15.99)], ["order_id", "customer_id", "product_id", "unit_price"]
        )
        result = compute_discount_pct(orders, raw_products)
        row = result.filter(F.col("order_id") == "O1").first()
        # base_price=19.99, unit_price=15.99 → discount > 0
        assert row["discount_pct"] > 0

    def test_no_discount_when_price_equal(self, spark, raw_products):
        orders = spark.createDataFrame(
            [("O1", "C1", "P001", 19.99)], ["order_id", "customer_id", "product_id", "unit_price"]
        )
        result = compute_discount_pct(orders, raw_products)
        assert result.first()["discount_pct"] == 0.0
