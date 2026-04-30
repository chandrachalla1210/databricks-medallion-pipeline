# Databricks notebook source
# src/dlt/silver_pipeline.py
"""
Silver layer – Delta Live Tables
Reads from Bronze tables, applies:
  - Type casting & standardisation
  - Deduplication (APPLY CHANGES)
  - Business rule validation
  - SCD Type-2 for customers
"""

# COMMAND ----------
import dlt                             # type: ignore
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, DateType, TimestampType, IntegerType

ENV          = spark.conf.get("env",          "dev")
CATALOG_NAME = spark.conf.get("catalog_name", "medallion_dev")
SILVER_SCHEMA = spark.conf.get("silver_schema", "silver")

VALID_STATUSES = ("pending", "processing", "shipped", "delivered", "cancelled", "returned")
VALID_REGIONS  = ("APAC", "EMEA", "AMER", "LATAM")


# ─────────────────────────────────────────────────────────────────────────────
# Silver – Orders (deduped, typed, validated)
# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="silver_orders",
    comment="Cleaned, deduplicated order records",
    table_properties={
        "quality":                        "silver",
        "delta.enableChangeDataFeed":     "true",
        "pipelines.autoOptimize.managed": "true",
    },
    partition_cols=["order_year", "region"],
)
@dlt.expect_or_fail("quantity_positive",   "quantity > 0")
@dlt.expect_or_fail("unit_price_positive", "unit_price > 0")
@dlt.expect("valid_status", f"status IN {VALID_STATUSES}")
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
        # ── Type casting ──────────────────────────────────────────────
        .withColumn("quantity",   F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
        .withColumn("order_date", F.to_date(F.col("order_date"), "yyyy-MM-dd"))
        .withColumn("order_year", F.year("order_date").cast("string"))
        # ── Derived fields ────────────────────────────────────────────
        .withColumn("line_total",      F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("status",          F.lower(F.trim(F.col("status"))))
        .withColumn("region",          F.upper(F.trim(F.col("region"))))
        # ── Dedup on event arrival order ──────────────────────────────
        .dropDuplicates(["order_id"])
        # ── Audit ─────────────────────────────────────────────────────
        .withColumn("_silver_ts", F.current_timestamp())
        .withColumn("_env",       F.lit(ENV))
    )


# ─────────────────────────────────────────────────────────────────────────────
# Silver – Customers (SCD Type 1 via APPLY CHANGES)
# ─────────────────────────────────────────────────────────────────────────────

dlt.create_streaming_table(
    name="silver_customers",
    comment="SCD-Type-1 customer dimension (latest record wins)",
    table_properties={
        "quality":                        "silver",
        "delta.enableChangeDataFeed":     "true",
        "pipelines.autoOptimize.managed": "true",
    },
)

dlt.apply_changes(
    target="silver_customers",
    source="bronze_customers",
    keys=["customer_id"],
    sequence_by=F.col("_ingest_ts"),
    stored_as_scd_type=1,
    apply_as_deletes=F.expr("status = 'DELETED'") if False else None,
    column_list=["customer_id", "first_name", "last_name", "email", "country", "signup_date"],
)


# ─────────────────────────────────────────────────────────────────────────────
# Silver – Products (streaming, cleaned)
# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="silver_products",
    comment="Cleaned product catalogue",
    table_properties={
        "quality":                        "silver",
        "delta.enableChangeDataFeed":     "true",
        "pipelines.autoOptimize.managed": "true",
    },
    partition_cols=["category"],
)
@dlt.expect_or_fail("product_id_present", "product_id IS NOT NULL")
@dlt.expect_or_fail("price_positive",     "base_price > 0")
def silver_products():
    return (
        dlt.read_stream("bronze_products")
        .withColumn("base_price", F.col("base_price").cast(DoubleType()))
        .withColumn("name",       F.trim(F.col("name")))
        .withColumn("category",   F.upper(F.trim(F.col("category"))))
        .dropDuplicates(["product_id"])
        .withColumn("_silver_ts", F.current_timestamp())
        .withColumn("_env",       F.lit(ENV))
    )


# ─────────────────────────────────────────────────────────────────────────────
# Silver – Enriched Orders (join orders × customers × products)
# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="silver_orders_enriched",
    comment="Orders enriched with customer & product dimensions",
    table_properties={
        "quality":                        "silver",
        "delta.enableChangeDataFeed":     "true",
        "pipelines.autoOptimize.managed": "true",
    },
    partition_cols=["order_year", "region"],
)
def silver_orders_enriched():
    orders    = dlt.read("silver_orders")
    customers = dlt.read("silver_customers")
    products  = dlt.read("silver_products")

    return (
        orders
        .join(customers.select("customer_id", "first_name", "last_name", "email", "country"),
              "customer_id", "left")
        .join(products.select("product_id", "name", "category", "base_price"),
              "product_id", "left")
        .withColumn("discount_pct",
            F.when(F.col("unit_price") < F.col("base_price"),
                   F.round((1 - F.col("unit_price") / F.col("base_price")) * 100, 2))
            .otherwise(F.lit(0.0)))
        .withColumn("_enriched_ts", F.current_timestamp())
    )
