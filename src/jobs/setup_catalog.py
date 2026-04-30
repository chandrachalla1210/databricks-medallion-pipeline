"""
Entry point: setup_catalog
Creates (if not exists) the Unity Catalog, schemas, and Volume.
"""
from __future__ import annotations
import argparse
import sys
from pyspark.sql import SparkSession
from src.utils.config import PipelineConfig
from src.utils.logger import get_logger


def _parse_args(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--env",           required=True)
    p.add_argument("--catalog_name",  required=True)
    p.add_argument("--schema_prefix", required=True)
    p.add_argument("--volume_name",   required=True)
    p.add_argument("--source_system", default="ecommerce")
    p.add_argument("--log_level",     default="INFO")
    return p.parse_args(argv)


def setup_unity_catalog(spark, cfg, logger):
    logger.info("Creating catalog: %s", cfg.catalog_name)
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{cfg.catalog_name}` COMMENT 'env={cfg.env}'")

    for layer, schema, comment in [
        ("bronze", cfg.bronze_schema, "Raw ingested data"),
        ("silver", cfg.silver_schema, "Cleansed and conformed data"),
        ("gold",   cfg.gold_schema,   "Business-ready aggregations"),
    ]:
        fqn = f"`{cfg.catalog_name}`.`{schema}`"
        logger.info("Creating schema: %s", fqn)
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fqn} COMMENT '{comment} env={cfg.env}'")
        spark.sql(f"""
            ALTER SCHEMA {fqn} SET TAGS (
                'layer'='{layer}', 'env'='{cfg.env}',
                'source_system'='{cfg.source_system}', 'managed_by'='dab'
            )
        """)

    # Raw-landing volume
    vol = f"`{cfg.catalog_name}`.`{cfg.bronze_schema}`.`{cfg.volume_name}`"
    logger.info("Creating volume: %s", vol)
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {vol} COMMENT 'Raw landing zone'")

    # Audit-log volume
    audit = f"`{cfg.catalog_name}`.`{cfg.bronze_schema}`.`audit_logs`"
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {audit} COMMENT 'Pipeline audit logs'")

    _grant_permissions(spark, cfg, logger)
    logger.info("Unity Catalog setup complete for env=%s", cfg.env)


def _grant_permissions(spark, cfg, logger):
    env_grants = {
        "dev":  [("data_engineers", "ALL PRIVILEGES ON CATALOG", cfg.catalog_name)],
        "test": [("data_engineers", "ALL PRIVILEGES ON CATALOG", cfg.catalog_name),
                 ("qa_team",        "USE CATALOG, SELECT ON CATALOG", cfg.catalog_name)],
        "int":  [("data_engineers", "ALL PRIVILEGES ON CATALOG", cfg.catalog_name),
                 ("analysts",       "USE CATALOG, SELECT ON CATALOG", cfg.catalog_name)],
        "prod": [("data_engineers", "ALL PRIVILEGES ON CATALOG", cfg.catalog_name),
                 ("analysts",       "USE CATALOG ON CATALOG", cfg.catalog_name)],
    }
    for principal, privilege, obj in env_grants.get(cfg.env, []):
        try:
            spark.sql(f"GRANT {privilege} `{obj}` TO `{principal}`")
            logger.info("Granted [%s] on %s to %s", privilege, obj, principal)
        except Exception as e:
            logger.warning("Grant failed: %s", e)


def main(argv=None):
    args = _parse_args(argv)
    cfg = PipelineConfig(
        env=args.env, catalog_name=args.catalog_name,
        schema_prefix=args.schema_prefix, source_system=args.source_system,
        volume_name=args.volume_name, log_level=args.log_level,
    )
    logger = get_logger(__name__, cfg.log_level)
    spark = SparkSession.builder.appName("MedallionCatalogSetup").getOrCreate()
    try:
        setup_unity_catalog(spark, cfg, logger)
    except Exception as exc:
        logger.exception("Catalog setup failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
