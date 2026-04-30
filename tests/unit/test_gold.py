# tests/unit/test_gold.py
"""
Unit tests for Gold layer aggregation logic.
Validates KPI correctness, segmentation, and output structure.
"""
from __future__ import annotations

import pytest
from datetime import date
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, DateType,
)

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))


# ─────────────────────────────────────────────────────────────────────────────
# Gold transformation helpers (mirror gold_pipeline.py logic)
# ─────────────────────────────────────────────────────────────────────────────

ACTIVE_STATUSES = ["delivered", "shipped", "processing"]


def compute_daily_sales(enriched_df):
    return (
        enriched_df
        .filter(F.col("status").isin(ACTIVE_STATUSES))
        .groupBy("order_date", "order_year", "region", "category")
        .agg(
            F.count("order_id")            .alias("total_orders"),
            F.sum("line_total")            .alias("gross_revenue"),
            F.sum(F.col("quantity"))       .alias("units_sold"),
            F.avg("unit_price")            .alias("avg_unit_price"),
            F.countDistinct("customer_id") .alias("unique_customers"),
        )
        .withColumn("gross_revenue",  F.round(F.col("gross_revenue"),  2))
        .withColumn("avg_unit_price", F.round(F.col("avg_unit_price"), 2))
    )


def compute_clv(orders_df, customers_df):
    clv_base = (
        orders_df
        .filter(F.col("status") == "delivered")
        .groupBy("customer_id")
        .agg(
            F.count("order_id")    .alias("total_orders"),
            F.sum("line_total")    .alias("lifetime_revenue"),
            F.avg("line_total")    .alias("avg_order_value"),
            F.min("order_date")    .alias("first_order_date"),
            F.max("order_date")    .alias("last_order_date"),
        )
    )
    return (
        clv_base
        .join(customers_df.select("customer_id", "first_name", "country"), "customer_id", "left")
        .withColumn("lifetime_revenue", F.round(F.col("lifetime_revenue"), 2))
        .withColumn("avg_order_value",  F.round(F.col("avg_order_value"),  2))
        .withColumn("clv_segment",
            F.when(F.col("lifetime_revenue") >= 10000, "Platinum")
            .when(F.col("lifetime_revenue") >= 5000,   "Gold")
            .when(F.col("lifetime_revenue") >= 1000,   "Silver")
            .otherwise("Bronze"))
    )


def compute_product_performance(enriched_df):
    return (
        enriched_df
        .filter(F.col("status").isin(["delivered", "shipped"]))
        .groupBy("product_id", "name", "category", "region")
        .agg(
            F.count("order_id")      .alias("order_count"),
            F.sum("quantity")        .alias("total_units_sold"),
            F.sum("line_total")      .alias("total_revenue"),
            F.countDistinct("customer_id").alias("unique_buyers"),
        )
        .withColumn("revenue_per_unit",
            F.round(F.col("total_revenue") / F.col("total_units_sold"), 2))
    )


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

ENRICHED_SCHEMA = StructType([
    StructField("order_id",    StringType(),  False),
    StructField("customer_id", StringType(),  True),
    StructField("product_id",  StringType(),  True),
    StructField("name",        StringType(),  True),
    StructField("category",    StringType(),  True),
    StructField("quantity",    IntegerType(), True),
    StructField("unit_price",  DoubleType(),  True),
    StructField("line_total",  DoubleType(),  True),
    StructField("order_date",  DateType(),    True),
    StructField("order_year",  StringType(),  True),
    StructField("status",      StringType(),  True),
    StructField("region",      StringType(),  True),
    StructField("discount_pct", DoubleType(), True),
])


@pytest.fixture
def enriched_orders(spark):
    rows = [
        ("O1", "C1", "P1", "Widget", "ELECTRONICS", 2, 50.0, 100.0,  date(2024, 1, 10), "2024", "delivered", "EMEA",  5.0),
        ("O2", "C2", "P1", "Widget", "ELECTRONICS", 1, 50.0,  50.0,  date(2024, 1, 10), "2024", "shipped",   "EMEA",  5.0),
        ("O3", "C1", "P2", "Shirt",  "APPAREL",     3, 30.0,  90.0,  date(2024, 1, 11), "2024", "delivered", "AMER",  0.0),
        ("O4", "C3", "P2", "Shirt",  "APPAREL",     1, 30.0,  30.0,  date(2024, 1, 11), "2024", "cancelled", "AMER",  0.0),  # excluded
        ("O5", "C2", "P1", "Widget", "ELECTRONICS", 2, 45.0,  90.0,  date(2024, 1, 12), "2024", "processing","APAC", 10.0),
    ]
    return spark.createDataFrame(rows, ENRICHED_SCHEMA)


@pytest.fixture
def customers_dim(spark):
    rows = [
        ("C1", "Alice", "US"),
        ("C2", "Bob",   "GB"),
        ("C3", "Carol", "DE"),
    ]
    return spark.createDataFrame(rows, ["customer_id", "first_name", "country"])


# ─────────────────────────────────────────────────────────────────────────────
# Tests – Daily Sales
# ─────────────────────────────────────────────────────────────────────────────

class TestGoldDailySales:

    def test_cancelled_excluded(self, enriched_orders):
        result = compute_daily_sales(enriched_orders)
        # O4 (cancelled) should not count
        total = result.agg(F.sum("total_orders")).first()[0]
        assert total == 4   # O1,O2,O3,O5

    def test_revenue_aggregated_per_day_region(self, enriched_orders):
        result = compute_daily_sales(enriched_orders)
        emea_jan10 = result.filter(
            (F.col("region") == "EMEA") & (F.col("order_date") == date(2024, 1, 10))
        ).first()
        assert emea_jan10 is not None
        assert emea_jan10["total_orders"] == 2
        assert emea_jan10["gross_revenue"] == pytest.approx(150.0, abs=0.01)

    def test_units_sold_correct(self, enriched_orders):
        result = compute_daily_sales(enriched_orders)
        total_units = result.agg(F.sum("units_sold")).first()[0]
        # O1=2, O2=1, O3=3, O5=2 = 8
        assert total_units == 8

    def test_unique_customers_counted(self, enriched_orders):
        result = compute_daily_sales(enriched_orders)
        emea_row = result.filter(F.col("region") == "EMEA").first()
        assert emea_row["unique_customers"] == 2   # C1, C2

    def test_no_null_revenue(self, enriched_orders):
        result = compute_daily_sales(enriched_orders)
        assert result.filter(F.col("gross_revenue").isNull()).count() == 0


# ─────────────────────────────────────────────────────────────────────────────
# Tests – Customer CLV
# ─────────────────────────────────────────────────────────────────────────────

class TestGoldCLV:

    def test_only_delivered_counted(self, enriched_orders, customers_dim):
        result = compute_clv(enriched_orders, customers_dim)
        # O1 (C1, delivered, 100), O3 (C1, delivered, 90), O2 (C2, shipped – excluded)
        c1_row = result.filter(F.col("customer_id") == "C1").first()
        assert c1_row["total_orders"]      == 2
        assert c1_row["lifetime_revenue"]  == pytest.approx(190.0, abs=0.01)

    def test_clv_segment_bronze(self, enriched_orders, customers_dim):
        result = compute_clv(enriched_orders, customers_dim)
        # All customers have < 1000 revenue → Bronze
        segments = {r["clv_segment"] for r in result.collect()}
        assert segments == {"Bronze"}

    def test_clv_segment_platinum(self, spark, customers_dim):
        rows = [("O1", "C1", "P1", "ELECTRONICS", 1, 12000.0, 12000.0,
                 date(2024, 1, 1), "2024", "delivered", "EMEA", 0.0)]
        df = spark.createDataFrame(rows, ENRICHED_SCHEMA)
        result = compute_clv(df, customers_dim)
        assert result.first()["clv_segment"] == "Platinum"

    def test_customer_name_joined(self, enriched_orders, customers_dim):
        result = compute_clv(enriched_orders, customers_dim)
        c1 = result.filter(F.col("customer_id") == "C1").first()
        assert c1["first_name"] == "Alice"
        assert c1["country"]    == "US"

    def test_avg_order_value_correct(self, enriched_orders, customers_dim):
        result = compute_clv(enriched_orders, customers_dim)
        c1 = result.filter(F.col("customer_id") == "C1").first()
        # (100 + 90) / 2 = 95
        assert c1["avg_order_value"] == pytest.approx(95.0, abs=0.01)


# ─────────────────────────────────────────────────────────────────────────────
# Tests – Product Performance
# ─────────────────────────────────────────────────────────────────────────────

class TestGoldProductPerformance:

    def test_cancelled_excluded(self, enriched_orders):
        result = compute_product_performance(enriched_orders)
        # Only delivered + shipped rows
        total_orders = result.agg(F.sum("order_count")).first()[0]
        # O1,O2 (P1/EMEA), O3 (P2/AMER), O5 (P1/APAC) = 4
        assert total_orders == 4

    def test_revenue_per_unit(self, enriched_orders):
        result = compute_product_performance(enriched_orders)
        p2_amer = result.filter(
            (F.col("product_id") == "P2") & (F.col("region") == "AMER")
        ).first()
        assert p2_amer is not None
        # total_revenue=90, total_units=3 → 30.0
        assert p2_amer["revenue_per_unit"] == pytest.approx(30.0, abs=0.01)

    def test_unique_buyers(self, enriched_orders):
        result = compute_product_performance(enriched_orders)
        p1_emea = result.filter(
            (F.col("product_id") == "P1") & (F.col("region") == "EMEA")
        ).first()
        # O1=C1, O2=C2
        assert p1_emea["unique_buyers"] == 2
