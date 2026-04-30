# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Ingestion
# MAGIC
# MAGIC Reads raw files from Unity Catalog Volume, adds metadata columns,
# MAGIC and writes as append-only Delta Live Tables with quarantine tables
# MAGIC for records that fail schema validation.
# MAGIC
# MAGIC **Config** is driven by the DLT pipeline configuration block (set via DAB).

# COMMAND ----------
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, TimestampType, IntegerType,
)

from src.utils.config import PipelineConfig
from src.utils.logger import get_logger

cfg = PipelineConfig.from_dlt_context()
logger = get_logger(__name__, cfg.log_level)

# ── Schemas ────────────────────────────────────────────────────────────────────
ORDERS_RAW_SCHEMA = StructType([
    StructField("order_id",      StringType(),    False),
    StructField("customer_id",   StringType(),    False),
    StructField("product_id",    StringType(),    False),
    StructField("quantity",      IntegerType(),   True),
    StructField("unit_price",    DoubleType(),    True),
    StructField("order_date",    StringType(),    True),   # cast to date in silver
    StructField("status",        StringType(),    True),
    StructField("store_id",      StringType(),    True),
])

CUSTOMERS_RAW_SCHEMA = StructType([
    StructField("customer_id",   StringType(),    False),
    StructField("first_name",    StringType(),    True),
    StructField("last_name",     StringType(),    True),
    StructField("email",         StringType(),    True),
    StructField("phone",         StringType(),    True),
    StructField("created_at",    StringType(),    True),
    StructField("country",       StringType(),    True),
    StructField("segment",       StringType(),    True),
])

PRODUCTS_RAW_SCHEMA = StructType([
    StructField("product_id",    StringType(),    False),
    StructField("product_name",  StringType(),    True),
    StructField("category",      StringType(),    True),
    StructField("sub_category",  StringType(),    True),
    StructField("cost_price",    DoubleType(),    True),
    StructField("list_price",    DoubleType(),    True),
    StructField("supplier_id",   StringType(),    True),
])

# ── Expectations (DLT data quality rules) ─────────────────────────────────────
ORDERS_EXPECTATIONS = {
    "order_id is not null":    "order_id IS NOT NULL",
    "customer_id is not null": "customer_id IS NOT NULL",
    "quantity is positive":    "quantity > 0",
    "unit_price is positive":  "unit_price > 0",
}

CUSTOMERS_EXPECTATIONS = {
    "customer_id is not null": "customer_id IS NOT NULL",
    "email is not null":       "email IS NOT NULL",
}

PRODUCTS_EXPECTATIONS = {
    "product_id is not null":  "product_id IS NOT NULL",
    "list_price is positive":  "list_price > 0",
}


# ── Helper: add metadata ───────────────────────────────────────────────────────
def _add_metadata(df):
    cols = [c for c in df.columns]
    return (
        df
        .withColumn("_ingested_at",   F.current_timestamp())
        .withColumn("_source_system", F.lit(cfg.source_system))
        .withColumn("_env",           F.lit(cfg.env))
        .withColumn("_row_hash",
                    F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in cols]), 256))
        .withColumn("_source_file",   F.input_file_name())
    )


# ── Bronze Tables ──────────────────────────────────────────────────────────────

@dlt.table(
    name="bronze_orders_raw",
    comment="Raw orders ingested from landing volume — no transformations applied",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true",
    },
)
@dlt.expect_all_or_drop(ORDERS_EXPECTATIONS)
def bronze_orders_raw():
    logger.info("Ingesting orders from volume: %s", cfg.volume_path("orders/"))
    return _add_metadata(
        spark.readStream
        .format("cloudFiles")                       # Auto Loader
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation",
                cfg.volume_path("_checkpoints/orders_schema"))
        .option("cloudFiles.inferColumnTypes", "true")
        .schema(ORDERS_RAW_SCHEMA)
        .load(cfg.volume_path("orders/"))
    )


@dlt.table(
    name="bronze_orders_quarantine",
    comment="Orders that failed bronze quality expectations",
    table_properties={"quality": "bronze_quarantine"},
)
@dlt.expect_all_or_drop(
    {f"NOT ({rule})": f"NOT ({expr})" for rule, expr in ORDERS_EXPECTATIONS.items()}
)
def bronze_orders_quarantine():
    """Captures records dropped by bronze_orders_raw for investigation."""
    return dlt.read_stream("bronze_orders_raw")


@dlt.table(
    name="bronze_customers_raw",
    comment="Raw customers ingested from landing volume",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true",
    },
)
@dlt.expect_all_or_drop(CUSTOMERS_EXPECTATIONS)
def bronze_customers_raw():
    logger.info("Ingesting customers from volume: %s", cfg.volume_path("customers/"))
    return _add_metadata(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation",
                cfg.volume_path("_checkpoints/customers_schema"))
        .schema(CUSTOMERS_RAW_SCHEMA)
        .load(cfg.volume_path("customers/"))
    )


@dlt.table(
    name="bronze_products_raw",
    comment="Raw products ingested from landing volume",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
    },
)
@dlt.expect_all_or_drop(PRODUCTS_EXPECTATIONS)
def bronze_products_raw():
    logger.info("Ingesting products from volume: %s", cfg.volume_path("products/"))
    return _add_metadata(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation",
                cfg.volume_path("_checkpoints/products_schema"))
        .schema(PRODUCTS_RAW_SCHEMA)
        .load(cfg.volume_path("products/"))
    )
