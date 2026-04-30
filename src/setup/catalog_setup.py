# Databricks notebook source
# src/setup/catalog_setup.py
"""
Bootstrap Unity Catalog resources:
  - Catalog
  - Schemas  (bronze, silver, gold, landing, dlt_schema)
  - Volumes  (landing/raw_files, bronze/checkpoints, silver/checkpoints, gold/exports)

All names are parameterised so the same notebook works across all envs.
Run as a Job task with base_parameters:
  catalog_name, env, bronze_schema, silver_schema, gold_schema, landing_schema
"""

# COMMAND ----------
# MAGIC %pip install databricks-sdk --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
import logging
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    CatalogInfo,
    SchemaInfo,
    VolumeInfo,
    VolumeType,
    SecurableType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)


# ── Parameters ────────────────────────────────────────────────────────────────
dbutils.widgets.text("catalog_name",   "medallion_dev",  "Catalog name")
dbutils.widgets.text("env",            "dev",            "Environment")
dbutils.widgets.text("bronze_schema",  "bronze",         "Bronze schema")
dbutils.widgets.text("silver_schema",  "silver",         "Silver schema")
dbutils.widgets.text("gold_schema",    "gold",           "Gold schema")
dbutils.widgets.text("landing_schema", "landing",        "Landing schema")

CATALOG_NAME   = dbutils.widgets.get("catalog_name")
ENV            = dbutils.widgets.get("env")
BRONZE_SCHEMA  = dbutils.widgets.get("bronze_schema")
SILVER_SCHEMA  = dbutils.widgets.get("silver_schema")
GOLD_SCHEMA    = dbutils.widgets.get("gold_schema")
LANDING_SCHEMA = dbutils.widgets.get("landing_schema")
DLT_SCHEMA     = "dlt_schema"

SCHEMAS = [LANDING_SCHEMA, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA, DLT_SCHEMA]

VOLUMES = [
    (LANDING_SCHEMA, "raw_files",          VolumeType.MANAGED),
    (BRONZE_SCHEMA,  "checkpoints",        VolumeType.MANAGED),
    (SILVER_SCHEMA,  "checkpoints",        VolumeType.MANAGED),
    (GOLD_SCHEMA,    "exports",            VolumeType.MANAGED),
    (GOLD_SCHEMA,    "checkpoints",        VolumeType.MANAGED),
]

# ── Helpers ───────────────────────────────────────────────────────────────────
w = WorkspaceClient()


def create_catalog_if_not_exists(catalog: str, env: str) -> None:
    try:
        w.catalogs.get(catalog)
        log.info("Catalog '%s' already exists – skipping creation.", catalog)
    except Exception:
        log.info("Creating catalog '%s' ...", catalog)
        w.catalogs.create(
            name=catalog,
            comment=f"Medallion pipeline catalog – {env}",
            properties={"environment": env, "owner": "data-engineering"},
        )
        log.info("Catalog '%s' created.", catalog)


def create_schema_if_not_exists(catalog: str, schema: str, env: str) -> None:
    full_name = f"{catalog}.{schema}"
    try:
        w.schemas.get(full_name)
        log.info("Schema '%s' already exists – skipping.", full_name)
    except Exception:
        log.info("Creating schema '%s' ...", full_name)
        w.schemas.create(
            catalog_name=catalog,
            name=schema,
            comment=f"{schema} layer – {env}",
            properties={"layer": schema, "environment": env},
        )
        log.info("Schema '%s' created.", full_name)


def create_volume_if_not_exists(catalog: str, schema: str, volume: str, vtype: VolumeType) -> None:
    full_name = f"{catalog}.{schema}.{volume}"
    try:
        w.volumes.read(full_name)
        log.info("Volume '%s' already exists – skipping.", full_name)
    except Exception:
        log.info("Creating volume '%s' ...", full_name)
        w.volumes.create(
            catalog_name=catalog,
            schema_name=schema,
            name=volume,
            volume_type=vtype,
            comment=f"{volume} volume in {schema}",
        )
        log.info("Volume '%s' created.", full_name)


# ── Execute ───────────────────────────────────────────────────────────────────
log.info("=== Catalog setup START (env=%s, catalog=%s) ===", ENV, CATALOG_NAME)

create_catalog_if_not_exists(CATALOG_NAME, ENV)

for schema in SCHEMAS:
    create_schema_if_not_exists(CATALOG_NAME, schema, ENV)

for schema, volume, vtype in VOLUMES:
    create_volume_if_not_exists(CATALOG_NAME, schema, volume, vtype)

log.info("=== Catalog setup COMPLETE ===")

# COMMAND ----------
# Verify – display all created resources
display(spark.sql(f"SHOW SCHEMAS IN {CATALOG_NAME}"))

# COMMAND ----------
display(spark.sql(f"SHOW VOLUMES IN {CATALOG_NAME}.{LANDING_SCHEMA}"))
