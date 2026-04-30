"""Unit tests for gold layer aggregation logic."""
import sys
import pytest
sys.path.insert(0, "src")
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import date


def _make_silver_orders(spark):
    schema = StructType([
        StructField("order_id",      StringType()),
        StructField("customer_id",   StringType()),
        StructField("product_id",    StringType()),
        StructField("quantity",      IntegerType()),
        StructField("unit_price",    DoubleType()),
        StructField("total_amount",  DoubleType()),
        StructField("order_date",    DateType()),
        StructField("status",        StringType()),
        StructField("store_id",      StringType()),
        StructField("category",      StringType()),
    ])
    data = [
        ("O001", "C1", "P1", 2, 50.0, 100.0, date(2024, 1, 15), "DELIVERED", "S1", "Electronics"),
        ("O002", "C2", "P2", 1, 100.0, 100.0, date(2024, 1, 16), "PENDING",   "S2", "Electronics"),
        ("O003", "C1", "P1", 3, 50.0, 150.0,  date(2024, 1, 15), "CANCELLED", "S1", "Electronics"),
        ("O004", "C3", "P1", 1, 50.0,  50.0,  date(2024, 1, 15), "DELIVERED", "S1", "Electronics"),
    ]
    return spark.createDataFrame(data, schema)


def _compute_daily_sales(df):
    return (
        df.groupBy("order_date", "store_id", "category")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_order_value"),
            F.sum("quantity").alias("units_sold"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum(F.when(F.col("status") == "CANCELLED", F.col("total_amount")).otherwise(0))
             .alias("cancelled_revenue"),
        )
        .withColumn("net_revenue", F.col("total_revenue") - F.col("cancelled_revenue"))
    )


class TestGoldDailySales:

    def test_aggregation_produces_correct_totals(self, spark):
        df = _make_silver_orders(spark)
        result = _compute_daily_sales(df)
        jan15_s1 = result.filter(
            (F.col("order_date") == date(2024, 1, 15)) & (F.col("store_id") == "S1")
        ).collect()[0]
        assert jan15_s1["order_count"]       == 3
        assert jan15_s1["total_revenue"]     == pytest.approx(300.0)  # 100+150+50
        assert jan15_s1["cancelled_revenue"] == pytest.approx(150.0)
        assert jan15_s1["net_revenue"]       == pytest.approx(150.0)
        assert jan15_s1["units_sold"]        == 6   # 2+3+1
        assert jan15_s1["unique_customers"]  == 2   # C1, C3

    def test_unique_customers_deduped(self, spark):
        df = _make_silver_orders(spark)
        result = _compute_daily_sales(df)
        jan15_s1 = result.filter(
            (F.col("order_date") == date(2024, 1, 15)) & (F.col("store_id") == "S1")
        ).collect()[0]
        # C1 placed 2 orders on that day, should count as 1 unique customer
        assert jan15_s1["unique_customers"] == 2

    def test_separate_dates_separate_rows(self, spark):
        df = _make_silver_orders(spark)
        result = _compute_daily_sales(df)
        dates = {r["order_date"] for r in result.collect()}
        assert date(2024, 1, 15) in dates
        assert date(2024, 1, 16) in dates


def _compute_clv(orders_df):
    return (
        orders_df
        .filter(F.col("status") != "CANCELLED")
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("total_amount").alias("lifetime_value"),
            F.avg("total_amount").alias("avg_order_value"),
        )
        .withColumn(
            "clv_segment",
            F.when(F.col("lifetime_value") >= 10000, "PLATINUM")
             .when(F.col("lifetime_value") >= 5000,  "GOLD")
             .when(F.col("lifetime_value") >= 1000,  "SILVER")
             .otherwise("BRONZE")
        )
    )


class TestGoldCustomerLifetimeValue:

    def test_cancelled_orders_excluded(self, spark):
        df = _make_silver_orders(spark)
        result = _compute_clv(df)
        # C1 has O001 (100 delivered) + O003 (150 cancelled) → only 100 counts
        c1 = result.filter(F.col("customer_id") == "C1").collect()[0]
        assert c1["lifetime_value"] == pytest.approx(100.0)
        assert c1["total_orders"]   == 1

    def test_clv_segment_assignment(self, spark):
        schema = StructType([
            StructField("order_id",     StringType()),
            StructField("customer_id",  StringType()),
            StructField("product_id",   StringType()),
            StructField("quantity",     IntegerType()),
            StructField("unit_price",   DoubleType()),
            StructField("total_amount", DoubleType()),
            StructField("order_date",   DateType()),
            StructField("status",       StringType()),
            StructField("store_id",     StringType()),
            StructField("category",     StringType()),
        ])
        high_value = [
            ("O100", "C_HV", "P1", 1, 1.0, 12000.0, date(2024,1,1), "DELIVERED", "S1", "X"),
        ]
        df = spark.createDataFrame(high_value, schema)
        result = _compute_clv(df)
        row = result.collect()[0]
        assert row["clv_segment"] == "PLATINUM"

    def test_clv_is_non_negative(self, spark):
        df = _make_silver_orders(spark)
        result = _compute_clv(df)
        for row in result.collect():
            assert row["lifetime_value"] >= 0
