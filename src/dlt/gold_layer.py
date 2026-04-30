# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Business-Ready Aggregations
# MAGIC
# MAGIC Reads from Silver tables and produces:
# MAGIC   - Daily sales summary (fact table)
# MAGIC   - Customer lifetime value metrics
# MAGIC   - Product performance analytics
# MAGIC   - Store revenue dashboard feed

# COMMAND ----------
import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.config import PipelineConfig
from src.utils.logger import get_logger

cfg = PipelineConfig.from_dlt_context()
logger = get_logger(__name__, cfg.log_level)

# ── Gold Expectations ──────────────────────────────────────────────────────────
GOLD_DAILY_SALES_EXPECTATIONS = {
    "total_revenue is non-negative": "total_revenue >= 0",
    "order_count is positive":       "order_count > 0",
}

GOLD_CLV_EXPECTATIONS = {
    "clv is non-negative":           "lifetime_value >= 0",
    "order_count is non-negative":   "total_orders >= 0",
}


# ── Gold Tables ────────────────────────────────────────────────────────────────

@dlt.table(
    name="gold_daily_sales",
    comment="Daily sales summary aggregated by date and store",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true",
    },
    partition_cols=["order_date"],
)
@dlt.expect_all_or_drop(GOLD_DAILY_SALES_EXPECTATIONS)
def gold_daily_sales():
    logger.info("Building gold_daily_sales")
    orders = dlt.read("silver_orders")
    products = dlt.read("silver_products")

    enriched = orders.join(products, on="product_id", how="left")

    return (
        enriched
        .groupBy("order_date", "store_id", "category")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_order_value"),
            F.sum("quantity").alias("units_sold"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum(
                F.when(F.col("status") == "CANCELLED", F.col("total_amount")).otherwise(0)
            ).alias("cancelled_revenue"),
        )
        .withColumn("net_revenue",   F.col("total_revenue") - F.col("cancelled_revenue"))
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("_env",          F.lit(cfg.env))
    )


@dlt.table(
    name="gold_customer_lifetime_value",
    comment="Customer lifetime value and behavioural segmentation",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
@dlt.expect_all_or_drop(GOLD_CLV_EXPECTATIONS)
def gold_customer_lifetime_value():
    logger.info("Building gold_customer_lifetime_value")
    orders    = dlt.read("silver_orders")
    customers = dlt.read("silver_customers_scd1")

    order_stats = (
        orders
        .filter(F.col("status") != "CANCELLED")
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("total_amount").alias("lifetime_value"),
            F.avg("total_amount").alias("avg_order_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.datediff(F.max("order_date"), F.min("order_date")).alias("customer_tenure_days"),
        )
        .withColumn(
            "purchase_frequency",
            F.when(F.col("customer_tenure_days") > 0,
                   F.round(F.col("total_orders") / (F.col("customer_tenure_days") / 30), 2)
                   ).otherwise(F.lit(1.0))
        )
        .withColumn(
            "clv_segment",
            F.when(F.col("lifetime_value") >= 10000, "PLATINUM")
            .when(F.col("lifetime_value") >= 5000,  "GOLD")
            .when(F.col("lifetime_value") >= 1000,  "SILVER")
            .otherwise("BRONZE")
        )
    )

    return (
        customers
        .join(order_stats, on="customer_id", how="left")
        .select(
            "customer_id", "full_name", "country", "segment",
            "total_orders", "lifetime_value", "avg_order_value",
            "first_order_date", "last_order_date",
            "customer_tenure_days", "purchase_frequency", "clv_segment",
            "_processed_at",
        )
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("_env", F.lit(cfg.env))
    )


@dlt.table(
    name="gold_product_performance",
    comment="Product-level revenue and margin analytics",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_product_performance():
    logger.info("Building gold_product_performance")
    orders   = dlt.read("silver_orders")
    products = dlt.read("silver_products")

    sales = (
        orders
        .filter(F.col("status") != "CANCELLED")
        .groupBy("product_id")
        .agg(
            F.sum("total_amount").alias("total_revenue"),
            F.sum("quantity").alias("total_units_sold"),
            F.count("order_id").alias("total_orders"),
            F.countDistinct("customer_id").alias("unique_buyers"),
        )
    )

    w = Window.orderBy(F.desc("total_revenue"))

    return (
        products
        .join(sales, on="product_id", how="left")
        .withColumn(
            "gross_profit",
            F.round(F.col("total_revenue") - (F.col("cost_price") * F.col("total_units_sold")), 2)
        )
        .withColumn(
            "gross_margin_pct",
            F.when(F.col("total_revenue") > 0,
                   F.round(F.col("gross_profit") / F.col("total_revenue") * 100, 2)
                   ).otherwise(F.lit(None))
        )
        .withColumn("revenue_rank",  F.rank().over(w))
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("_env",          F.lit(cfg.env))
    )
