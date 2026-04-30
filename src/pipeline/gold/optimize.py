# Databricks notebook source
# src/pipeline/gold/optimize.py
"""
OPTIMIZE + ZORDER gold tables for optimal BI query performance.
Run as a post-DLT task in the orchestration job.
"""

# COMMAND ----------
import logging
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── Parameters ────────────────────────────────────────────────────────────────
dbutils.widgets.text("catalog_name", "medallion_dev", "Catalog")
dbutils.widgets.text("env",          "dev",           "Environment")

CATALOG = dbutils.widgets.get("catalog_name")
ENV     = dbutils.widgets.get("env")
SCHEMA  = "dlt_schema"

# ── Table → ZORDER column map ─────────────────────────────────────────────────
@dataclass
class TableOpt:
    table:       str
    zorder_cols: list[str]
    vacuum_hours: int = 168   # 7 days

GOLD_TABLES = [
    TableOpt("gold_daily_sales",         ["order_date", "region", "category"]),
    TableOpt("gold_customer_clv",        ["customer_id", "clv_segment"]),
    TableOpt("gold_product_performance", ["product_id", "category"]),
    TableOpt("gold_regional_kpis",       ["order_year", "order_month", "region"]),
]


# COMMAND ----------
def optimize_table(catalog: str, schema: str, t: TableOpt) -> None:
    full = f"{catalog}.{schema}.{t.table}"
    zorder_clause = ", ".join(t.zorder_cols)
    log.info("OPTIMIZE %s ZORDER BY (%s) …", full, zorder_clause)
    spark.sql(f"OPTIMIZE {full} ZORDER BY ({zorder_clause})")
    log.info("VACUUM %s RETAIN %d HOURS …", full, t.vacuum_hours)
    spark.sql(f"VACUUM {full} RETAIN {t.vacuum_hours} HOURS")
    log.info("Done: %s", full)


log.info("=== Gold OPTIMIZE START (env=%s) ===", ENV)
for tbl in GOLD_TABLES:
    optimize_table(CATALOG, SCHEMA, tbl)
log.info("=== Gold OPTIMIZE COMPLETE ===")
