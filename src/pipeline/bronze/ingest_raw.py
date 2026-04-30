# Databricks notebook source
# src/pipeline/bronze/ingest_raw.py
"""
Raw data ingestion task.
Copies / validates files into the landing Volume before the DLT pipeline runs.
In a real scenario this would pull from Kafka, S3, ADLS, SFTP, APIs etc.
Here we generate synthetic data so the demo is self-contained.
"""

# COMMAND ----------
import logging
from datetime import date, timedelta
import random
import json
from pathlib import Path

from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── Parameters ────────────────────────────────────────────────────────────────
dbutils.widgets.text("catalog_name", "medallion_dev", "Catalog")
dbutils.widgets.text("env",          "dev",           "Environment")
dbutils.widgets.text("source_path",  "",              "Override source path (optional)")
dbutils.widgets.text("num_orders",   "500",           "Number of synthetic orders to generate")

CATALOG_NAME = dbutils.widgets.get("catalog_name")
ENV          = dbutils.widgets.get("env")
NUM_ORDERS   = int(dbutils.widgets.get("num_orders"))

# Resolve landing path
SOURCE_PATH  = dbutils.widgets.get("source_path").strip()
LANDING_PATH = SOURCE_PATH or f"/Volumes/{CATALOG_NAME}/landing/raw_files"

log.info("Landing path: %s  | orders: %d", LANDING_PATH, NUM_ORDERS)

# COMMAND ----------
# ── Synthetic data generation ─────────────────────────────────────────────────

REGIONS   = ["APAC", "EMEA", "AMER", "LATAM"]
STATUSES  = ["pending", "processing", "shipped", "delivered", "cancelled"]
CATEGORIES = ["ELECTRONICS", "APPAREL", "FOOD", "BOOKS", "SPORTS"]

random.seed(42)

# Products
products = [
    {"product_id": f"P{i:04d}", "name": f"Product {i}", "category": random.choice(CATEGORIES),
     "base_price": round(random.uniform(5, 500), 2)}
    for i in range(1, 51)
]

# Customers
customers = [
    {"customer_id": f"C{i:06d}", "first_name": f"First{i}", "last_name": f"Last{i}",
     "email": f"user{i}@example.com", "country": random.choice(["US", "GB", "DE", "JP", "AU"]),
     "signup_date": str(date(2020, 1, 1) + timedelta(days=random.randint(0, 1000)))}
    for i in range(1, 101)
]

# Orders
today = date.today()
orders = []
for i in range(1, NUM_ORDERS + 1):
    p = random.choice(products)
    orders.append({
        "order_id":    f"ORD{i:08d}",
        "customer_id": random.choice(customers)["customer_id"],
        "product_id":  p["product_id"],
        "quantity":    random.randint(1, 20),
        "unit_price":  round(p["base_price"] * random.uniform(0.8, 1.1), 2),
        "order_date":  str(today - timedelta(days=random.randint(0, 365))),
        "status":      random.choice(STATUSES),
        "region":      random.choice(REGIONS),
    })

# COMMAND ----------
# ── Write to landing Volume ────────────────────────────────────────────────────

def write_json_lines(records: list[dict], path: str) -> None:
    df = spark.createDataFrame(records)
    (df.coalesce(1)
       .write.mode("overwrite")
       .json(path))
    log.info("Written %d records → %s", len(records), path)

def write_csv(records: list[dict], path: str) -> None:
    df = spark.createDataFrame(records)
    (df.coalesce(1)
       .write.mode("overwrite")
       .option("header", "true")
       .csv(path))
    log.info("Written %d records → %s", len(records), path)


write_json_lines(orders,    f"{LANDING_PATH}/orders")
write_csv(customers,        f"{LANDING_PATH}/customers")
write_json_lines(products,  f"{LANDING_PATH}/products")

log.info("Ingestion complete. Orders=%d, Customers=%d, Products=%d",
         len(orders), len(customers), len(products))

# COMMAND ----------
# Verify
for sub in ["orders", "customers", "products"]:
    count = spark.read.format("cloudFiles" if False else "json" if sub != "customers" else "csv") \
                      .option("header", "true").load(f"{LANDING_PATH}/{sub}").count()
    log.info("  %s: %d records", sub, count)
