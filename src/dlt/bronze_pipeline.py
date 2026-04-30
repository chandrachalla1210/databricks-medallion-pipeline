# Databricks notebook source
# src/dlt/bronze_pipeline.py
"""
Bronze layer – Delta Live Tables
Reads raw files from the landing Volume and creates raw streaming tables.

DLT pipeline configuration (passed via pipeline config):
  env, catalog_name, source_path, bronze_schema
"""

# COMMAND ----------
import dlt                                           # type: ignore  (DLT runtime)
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType, DateType,
)

# ── Runtime config injected by DLT pipeline ───────────────────────────────────
ENV          = spark.conf.get("env",          "dev")
CATALOG_NAME = spark.conf.get("catalog_name", "medallion_dev")
SOURCE_PATH  = spark.conf.get("source_path",  f"/Volumes/{CATALOG_NAME}/landing/raw_files")


# ─────────────────────────────────────────────────────────────────────────────
# Schema definitions  (keeps notebooks schema-aware without hard-coding)
# ─────────────────────────────────────────────────────────────────────────────

ORDERS_RAW_SCHEMA = StructType([
    StructField("order_id",      StringType(),    False),
    StructField("customer_id",   StringType(),    False),
    StructField("product_id",    StringType(),    False),
    StructField("quantity",      IntegerType(),   True),
    StructField("unit_price",    DoubleType(),    True),
    StructField("order_date",    StringType(),    True),   # raw = string
    StructField("status",        StringType(),    True),
    StructField("region",        StringType(),    True),
])

CUSTOMERS_RAW_SCHEMA = StructType([
    StructField("customer_id",   StringType(),    False),
    StructField("first_name",    StringType(),    True),
    StructField("last_name",     StringType(),    True),
    StructField("email",         StringType(),    True),
    StructField("country",       StringType(),    True),
    StructField("signup_date",   StringType(),    True),
])

PRODUCTS_RAW_SCHEMA = StructType([
    StructField("product_id",    StringType(),    False),
    StructField("name",          StringType(),    True),
    StructField("category",      StringType(),    True),
    StructField("base_price",    DoubleType(),    True),
])


# ─────────────────────────────────────────────────────────────────────────────
# Auto Loader helper
# ─────────────────────────────────────────────────────────────────────────────

def _auto_loader(source_dir: str, schema: StructType, format: str = "json") -> "DataFrame":
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", format)
        .option("cloudFiles.schemaLocation", f"{source_dir}/_schema")
        .option("cloudFiles.inferColumnTypes", "false")
        .schema(schema)
        .load(source_dir)
        .withColumn("_source_file",   F.input_file_name())
        .withColumn("_ingest_ts",     F.current_timestamp())
        .withColumn("_env",           F.lit(ENV))
    )


# ─────────────────────────────────────────────────────────────────────────────
# Bronze – Orders (streaming)
# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="bronze_orders",
    comment="Raw orders ingested from landing zone via Auto Loader",
    table_properties={
        "quality":                   "bronze",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.managed": "true",
    },
    partition_cols=["region"],
)
@dlt.expect("order_id_not_null",   "order_id IS NOT NULL")
@dlt.expect("customer_id_present", "customer_id IS NOT NULL")
def bronze_orders():
    return _auto_loader(f"{SOURCE_PATH}/orders", ORDERS_RAW_SCHEMA, "json")


# ─────────────────────────────────────────────────────────────────────────────
# Bronze – Customers (streaming)
# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="bronze_customers",
    comment="Raw customer records from landing zone",
    table_properties={
        "quality":                        "bronze",
        "delta.enableChangeDataFeed":     "true",
        "pipelines.autoOptimize.managed": "true",
    },
)
@dlt.expect("customer_id_present", "customer_id IS NOT NULL")
@dlt.expect("email_format",        "email LIKE '%@%'")
def bronze_customers():
    return _auto_loader(f"{SOURCE_PATH}/customers", CUSTOMERS_RAW_SCHEMA, "csv")


# ─────────────────────────────────────────────────────────────────────────────
# Bronze – Products (streaming)
# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="bronze_products",
    comment="Raw product catalogue from landing zone",
    table_properties={
        "quality":                        "bronze",
        "delta.enableChangeDataFeed":     "true",
        "pipelines.autoOptimize.managed": "true",
    },
)
@dlt.expect("product_id_present", "product_id IS NOT NULL")
@dlt.expect("price_positive",     "base_price > 0")
def bronze_products():
    return _auto_loader(f"{SOURCE_PATH}/products", PRODUCTS_RAW_SCHEMA, "json")


# ─────────────────────────────────────────────────────────────────────────────
# Bronze – Quarantine (rows that fail expectations land here)
# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="bronze_orders_quarantine",
    comment="Orders that failed bronze data quality expectations",
    table_properties={"quality": "quarantine"},
)
@dlt.expect_or_drop("order_id_not_null",   "order_id IS NOT NULL")
@dlt.expect_or_drop("customer_id_present", "customer_id IS NOT NULL")
def bronze_orders_quarantine():
    return (
        dlt.read_stream("bronze_orders")
        .filter(F.col("order_id").isNull() | F.col("customer_id").isNull())
        .withColumn("_quarantine_reason", F.lit("null key field"))
    )
