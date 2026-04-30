"""Entry point: run_quality_checks — post-DLT data quality validation on Gold tables."""
from __future__ import annotations
import argparse, sys, json
from pyspark.sql import SparkSession
from src.utils.config import PipelineConfig
from src.utils.helpers import compute_null_percentages
from src.utils.logger import get_logger

NULL_THRESHOLD_PCT = 5.0   # Fail if any critical column exceeds 5% nulls

GOLD_QUALITY_RULES = {
    "gold_daily_sales": {
        "critical_columns": ["order_date", "total_revenue", "order_count"],
        "min_row_count": 1,
        "checks": [
            ("total_revenue >= 0", "total_revenue >= 0"),
            ("order_count > 0",    "order_count > 0"),
        ],
    },
    "gold_customer_lifetime_value": {
        "critical_columns": ["customer_id", "lifetime_value"],
        "min_row_count": 1,
        "checks": [
            ("lifetime_value >= 0", "lifetime_value >= 0"),
        ],
    },
    "gold_product_performance": {
        "critical_columns": ["product_id", "total_revenue"],
        "min_row_count": 1,
        "checks": [],
    },
}


def _parse_args(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--env");           p.add_argument("--catalog_name")
    p.add_argument("--schema_prefix"); p.add_argument("--source_system", default="ecommerce")
    p.add_argument("--volume_name",    default="raw_landing")
    p.add_argument("--run_date",       default="")
    p.add_argument("--log_level",      default="INFO")
    return p.parse_args(argv)


def run_quality_checks(spark, cfg, logger):
    failures = []

    for table, rules in GOLD_QUALITY_RULES.items():
        fqn = cfg.full_table(cfg.gold_schema, table)
        logger.info("Quality check: %s", fqn)

        try:
            df = spark.read.table(fqn)
        except Exception as e:
            failures.append(f"{table}: table not readable — {e}")
            continue

        row_count = df.count()
        if row_count < rules["min_row_count"]:
            failures.append(f"{table}: row count {row_count} < minimum {rules['min_row_count']}")

        null_pcts = compute_null_percentages(df.select(rules["critical_columns"]))
        for col, pct in null_pcts.items():
            if pct > NULL_THRESHOLD_PCT:
                failures.append(f"{table}.{col}: {pct}% nulls > threshold {NULL_THRESHOLD_PCT}%")

        for check_name, sql_expr in rules["checks"]:
            failing = df.filter(f"NOT ({sql_expr})").count()
            if failing > 0:
                failures.append(f"{table} [{check_name}]: {failing} rows failed")

    if failures:
        logger.error("Quality checks FAILED:\n%s", "\n".join(failures))
        raise RuntimeError(f"Data quality failures: {failures}")

    logger.info("All quality checks passed")


def main(argv=None):
    args = _parse_args(argv)
    cfg = PipelineConfig(
        env=args.env, catalog_name=args.catalog_name,
        schema_prefix=args.schema_prefix, source_system=args.source_system,
        volume_name=args.volume_name, log_level=args.log_level,
    )
    logger = get_logger(__name__, cfg.log_level)
    spark = SparkSession.builder.appName("MedallionQualityChecks").getOrCreate()
    try:
        run_quality_checks(spark, cfg, logger)
    except Exception as exc:
        logger.exception("Quality checks failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
