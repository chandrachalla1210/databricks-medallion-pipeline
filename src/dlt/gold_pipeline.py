# Databricks notebook source
# src/dlt/gold_pipeline.py
"""
Gold layer – Delta Live Tables
Business-level aggregations & serving tables consumed by BI / ML.
"""

# COMMAND ----------
import dlt                             # type: ignore
from pyspark.sql import functions as F

ENV          = spark.conf.get("env",          "dev")
CATALOG_NAME = spark.conf.get("catalog_name", "medallion_dev")


# ─────────────────────────────────────────────────────────────────────────────
# Gold – Daily Sales Summary
# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="gold_daily_sales",
    comment="Daily order revenue and volume KPIs",
    table_properties={
        "quality":                        "gold",
        "delta.enableChangeDataFeed":     "true",
        "pipelines.autoOptimize.managed": "true",
    },
    partition_cols=["order_year", "region"],
)
def gold_daily_sales():
    return (
        dlt.read("silver_orders_enriched")
        .filter(F.col("status").isin(["delivered", "shipped", "processing"]))
        .groupBy("order_date", "order_year", "region", "category")
        .agg(
            F.count("order_id")            .alias("total_orders"),
            F.sum("line_total")            .alias("gross_revenue"),
            F.sum(F.col("quantity"))       .alias("units_sold"),
            F.avg("unit_price")            .alias("avg_unit_price"),
            F.avg("discount_pct")          .alias("avg_discount_pct"),
            F.countDistinct("customer_id") .alias("unique_customers"),
        )
        .withColumn("gross_revenue",     F.round(F.col("gross_revenue"),     2))
        .withColumn("avg_unit_price",    F.round(F.col("avg_unit_price"),    2))
        .withColumn("avg_discount_pct",  F.round(F.col("avg_discount_pct"),  2))
        .withColumn("_gold_ts",          F.current_timestamp())
        .withColumn("_env",              F.lit(ENV))
    )


# ─────────────────────────────────────────────────────────────────────────────
# Gold – Customer Lifetime Value (CLV) Snapshot
# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="gold_customer_clv",
    comment="Customer lifetime value snapshot – refreshed each pipeline run",
    table_properties={
        "quality":                        "gold",
        "delta.enableChangeDataFeed":     "true",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_customer_clv():
    orders    = dlt.read("silver_orders_enriched")
    customers = dlt.read("silver_customers")

    clv = (
        orders
        .filter(F.col("status").isin(["delivered"]))
        .groupBy("customer_id")
        .agg(
            F.count("order_id")        .alias("total_orders"),
            F.sum("line_total")        .alias("lifetime_revenue"),
            F.avg("line_total")        .alias("avg_order_value"),
            F.min("order_date")        .alias("first_order_date"),
            F.max("order_date")        .alias("last_order_date"),
            F.countDistinct("category").alias("distinct_categories"),
        )
    )

    return (
        clv
        .join(customers.select("customer_id", "first_name", "last_name", "email", "country"),
              "customer_id", "left")
        .withColumn("lifetime_revenue",    F.round(F.col("lifetime_revenue"),    2))
        .withColumn("avg_order_value",     F.round(F.col("avg_order_value"),     2))
        .withColumn("days_active",
            F.datediff(F.col("last_order_date"), F.col("first_order_date")))
        .withColumn("clv_segment",
            F.when(F.col("lifetime_revenue") >= 10000, "Platinum")
            .when(F.col("lifetime_revenue") >= 5000,  "Gold")
            .when(F.col("lifetime_revenue") >= 1000,  "Silver")
            .otherwise("Bronze"))
        .withColumn("_gold_ts", F.current_timestamp())
        .withColumn("_env",     F.lit(ENV))
    )


# ─────────────────────────────────────────────────────────────────────────────
# Gold – Product Performance
# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="gold_product_performance",
    comment="Product-level sales performance aggregated across all time",
    table_properties={
        "quality":                        "gold",
        "delta.enableChangeDataFeed":     "true",
        "pipelines.autoOptimize.managed": "true",
    },
    partition_cols=["category"],
)
def gold_product_performance():
    return (
        dlt.read("silver_orders_enriched")
        .filter(F.col("status").isin(["delivered", "shipped"]))
        .groupBy("product_id", "name", "category", "region")
        .agg(
            F.count("order_id")        .alias("order_count"),
            F.sum("quantity")          .alias("total_units_sold"),
            F.sum("line_total")        .alias("total_revenue"),
            F.avg("discount_pct")      .alias("avg_discount_pct"),
            F.countDistinct("customer_id").alias("unique_buyers"),
        )
        .withColumn("total_revenue",    F.round(F.col("total_revenue"),    2))
        .withColumn("avg_discount_pct", F.round(F.col("avg_discount_pct"), 2))
        .withColumn("revenue_per_unit",
            F.round(F.col("total_revenue") / F.col("total_units_sold"), 2))
        .withColumn("_gold_ts", F.current_timestamp())
        .withColumn("_env",     F.lit(ENV))
    )


# ─────────────────────────────────────────────────────────────────────────────
# Gold – Regional KPIs  (flattened, BI-ready)
# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="gold_regional_kpis",
    comment="Monthly KPIs per region for executive dashboards",
    table_properties={
        "quality":                        "gold",
        "delta.enableChangeDataFeed":     "true",
        "pipelines.autoOptimize.managed": "true",
    },
    partition_cols=["order_year"],
)
def gold_regional_kpis():
    daily = dlt.read("gold_daily_sales")
    return (
        daily
        .groupBy("order_year",
                 F.month("order_date").alias("order_month"),
                 "region")
        .agg(
            F.sum("gross_revenue")     .alias("monthly_revenue"),
            F.sum("total_orders")      .alias("monthly_orders"),
            F.sum("units_sold")        .alias("monthly_units_sold"),
            F.sum("unique_customers")  .alias("monthly_unique_customers"),
            F.avg("avg_discount_pct")  .alias("avg_discount_pct"),
        )
        .withColumn("monthly_revenue",           F.round(F.col("monthly_revenue"),           2))
        .withColumn("avg_discount_pct",          F.round(F.col("avg_discount_pct"),          2))
        .withColumn("revenue_per_order",
            F.round(F.col("monthly_revenue") / F.col("monthly_orders"), 2))
        .withColumn("_gold_ts", F.current_timestamp())
        .withColumn("_env",     F.lit(ENV))
    )
