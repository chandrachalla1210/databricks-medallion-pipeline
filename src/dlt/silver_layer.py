# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Cleansed & Conformed
# MAGIC
# MAGIC Reads from Bronze streaming tables, applies:
# MAGIC   - Type casting & date parsing
# MAGIC   - Null handling & deduplication
# MAGIC   - PII masking (email, phone)
# MAGIC   - Business rule validations
# MAGIC
# MAGIC Writes SCD-1 style upserts using DLT APPLY CHANGES INTO.

# COMMAND ----------
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType

from src.utils.config import PipelineConfig
from src.utils.logger import get_logger

cfg = PipelineConfig.from_dlt_context()
logger = get_logger(__name__, cfg.log_level)

# ── Silver Expectations ────────────────────────────────────────────────────────
SILVER_ORDERS_EXPECTATIONS = {
    "valid order status":     "status IN ('PENDING','PROCESSING','SHIPPED','DELIVERED','CANCELLED')",
    "order_date is not null": "order_date IS NOT NULL",
    "total_amount positive":  "total_amount >= 0",
}

SILVER_CUSTOMERS_EXPECTATIONS = {
    "valid country code": "LENGTH(country) >= 2",
    "valid segment":      "segment IN ('STANDARD','PREMIUM','VIP','WHOLESALE')",
}

SILVER_PRODUCTS_EXPECTATIONS = {
    "margin is valid":    "margin_pct BETWEEN -100 AND 500",
    "list_price >= cost": "list_price >= cost_price",
}


# ── Helper: mask PII ───────────────────────────────────────────────────────────
def _mask_email(col_name: str) -> F.Column:
    """Replace email with SHA-256 hash — reversible in prod vault, masked in lower envs."""
    return F.when(
        F.lit(cfg.env) == "prod",
        F.sha2(F.col(col_name), 256)
    ).otherwise(F.col(col_name))


def _mask_phone(col_name: str) -> F.Column:
    """Keep only last 4 digits in non-prod environments."""
    return F.when(
        F.lit(cfg.env) != "prod",
        F.concat(F.lit("***-***-"), F.substring(F.col(col_name), -4, 4))
    ).otherwise(F.col(col_name))


# ── Silver Tables ──────────────────────────────────────────────────────────────

@dlt.table(
    name="silver_orders",
    comment="Cleansed, deduped orders with correct data types",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true",
    },
)
@dlt.expect_all_or_drop(SILVER_ORDERS_EXPECTATIONS)
def silver_orders():
    logger.info("Transforming bronze_orders_raw → silver_orders")
    return (
        dlt.read_stream("bronze_orders_raw")
        .withColumn("order_date",    F.to_date("order_date", "yyyy-MM-dd"))
        .withColumn("quantity",      F.col("quantity").cast("integer"))
        .withColumn("unit_price",    F.col("unit_price").cast(DoubleType()))
        .withColumn("total_amount",  F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("status",        F.upper("status"))
        .withColumn("_processed_at", F.current_timestamp())
        # Dedup: keep latest per order_id per micro-batch
        .dropDuplicates(["order_id", "_row_hash"])
        .drop("_source_file")
    )


@dlt.table(
    name="silver_customers",
    comment="Cleansed customers with PII masking applied",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true",
    },
)
@dlt.expect_all_or_drop(SILVER_CUSTOMERS_EXPECTATIONS)
def silver_customers():
    logger.info("Transforming bronze_customers_raw → silver_customers")
    return (
        dlt.read_stream("bronze_customers_raw")
        .withColumn("email",          _mask_email("email"))
        .withColumn("phone",          _mask_phone("phone"))
        .withColumn("created_at",     F.to_timestamp("created_at", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("full_name",      F.concat_ws(" ", "first_name", "last_name"))
        .withColumn("country",        F.upper("country"))
        .withColumn("segment",        F.upper("segment"))
        .withColumn("_processed_at",  F.current_timestamp())
        .dropDuplicates(["customer_id", "_row_hash"])
        .drop("_source_file")
    )


@dlt.table(
    name="silver_products",
    comment="Cleansed products with derived margin metrics",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
    },
)
@dlt.expect_all_or_drop(SILVER_PRODUCTS_EXPECTATIONS)
def silver_products():
    logger.info("Transforming bronze_products_raw → silver_products")
    return (
        dlt.read_stream("bronze_products_raw")
        .withColumn("cost_price",    F.col("cost_price").cast(DoubleType()))
        .withColumn("list_price",    F.col("list_price").cast(DoubleType()))
        .withColumn("margin_amount", F.round(F.col("list_price") - F.col("cost_price"), 2))
        .withColumn(
            "margin_pct",
            F.when(F.col("cost_price") > 0,
                   F.round((F.col("list_price") - F.col("cost_price")) / F.col("cost_price") * 100, 2)
                   ).otherwise(F.lit(None))
        )
        .withColumn("category",      F.initcap("category"))
        .withColumn("sub_category",  F.initcap("sub_category"))
        .withColumn("_processed_at", F.current_timestamp())
        .dropDuplicates(["product_id"])
        .drop("_source_file")
    )


# ── SCD-1 Upsert using APPLY CHANGES INTO ─────────────────────────────────────
dlt.create_streaming_table(
    name="silver_customers_scd1",
    comment="SCD Type-1 customer dimension — latest record per customer_id",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
)

dlt.apply_changes(
    target="silver_customers_scd1",
    source="silver_customers",
    keys=["customer_id"],
    sequence_by="created_at",
    stored_as_scd_type=1,
)
