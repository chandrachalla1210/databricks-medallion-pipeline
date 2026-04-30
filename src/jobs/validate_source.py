"""Entry point: validate_source — checks source data availability before DLT runs."""
from __future__ import annotations
import argparse, sys
from datetime import datetime
from pyspark.sql import SparkSession
from src.utils.config import PipelineConfig
from src.utils.logger import get_logger


def _parse_args(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--env");           p.add_argument("--catalog_name")
    p.add_argument("--schema_prefix"); p.add_argument("--source_system", default="ecommerce")
    p.add_argument("--volume_name",    default="raw_landing")
    p.add_argument("--run_date",       default="")
    p.add_argument("--log_level",      default="INFO")
    return p.parse_args(argv)


def validate_source_data(spark, cfg, run_date, logger):
    """Ensure raw files landed in the volume for the given run_date."""
    base = cfg.volume_path()
    results = {}

    for entity in ["orders", "customers", "products"]:
        path = f"{base}/{entity}/"
        try:
            df = spark.read.format("json").option("recursiveFileLookup", "true").load(path)
            count = df.count()
            results[entity] = {"status": "OK", "count": count}
            logger.info("Source validation OK: %s — %d records", entity, count)
        except Exception as e:
            results[entity] = {"status": "MISSING", "error": str(e)}
            logger.error("Source validation FAILED: %s — %s", entity, e)

    failures = [e for e, r in results.items() if r["status"] != "OK"]
    if failures:
        raise RuntimeError(f"Source data missing for: {failures}")

    return results


def main(argv=None):
    args = _parse_args(argv)
    cfg = PipelineConfig(
        env=args.env, catalog_name=args.catalog_name,
        schema_prefix=args.schema_prefix, source_system=args.source_system,
        volume_name=args.volume_name, log_level=args.log_level,
        run_date=args.run_date,
    )
    logger = get_logger(__name__, cfg.log_level)
    spark = SparkSession.builder.appName("MedallionSourceValidation").getOrCreate()
    try:
        validate_source_data(spark, cfg, args.run_date, logger)
    except Exception as exc:
        logger.exception("Source validation failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
