"""
Reusable helper functions shared across bronze, silver, and gold layers.
Pure Python / PySpark — no Databricks-specific imports at module level so
that unit tests can run without a live Spark session.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from .logger import get_logger

logger = get_logger(__name__)


# ── Table & schema helpers ────────────────────────────────────────────────────

def get_full_table_name(catalog: str, schema: str, table: str) -> str:
    """Return three-part Unity Catalog identifier."""
    return f"`{catalog}`.`{schema}`.`{table}`"


def validate_dataframe_schema(
    df: DataFrame,
    expected_columns: List[str],
    raise_on_missing: bool = True,
) -> Dict[str, Any]:
    """
    Check that *df* contains all *expected_columns*.

    Returns
    -------
    dict with keys:
      - valid (bool)
      - missing_columns (list)
      - extra_columns (list)
    """
    actual = set(df.columns)
    expected = set(expected_columns)
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)

    result = {"valid": len(missing) == 0, "missing_columns": missing, "extra_columns": extra}

    if missing and raise_on_missing:
        raise ValueError(
            f"DataFrame is missing required columns: {missing}. "
            f"Extra columns present: {extra}"
        )
    return result


# ── Ingestion helpers ─────────────────────────────────────────────────────────

def read_volume_files(
    spark: SparkSession,
    volume_path: str,
    format: str = "json",
    schema: Optional[StructType] = None,
    options: Optional[Dict[str, str]] = None,
) -> DataFrame:
    """
    Read files from a Unity Catalog volume into a DataFrame.

    Parameters
    ----------
    spark       : Active SparkSession
    volume_path : Full volume path, e.g. /Volumes/catalog/schema/volume/subdir
    format      : File format (json | csv | parquet | avro)
    schema      : Optional explicit schema (avoids schema inference cost)
    options     : Extra reader options (e.g. header, delimiter)
    """
    reader = spark.read.format(format)
    if schema:
        reader = reader.schema(schema)
    if options:
        for k, v in options.items():
            reader = reader.option(k, v)

    logger.info("Reading %s files from %s", format, volume_path)
    return reader.load(volume_path)


def add_metadata_columns(df: DataFrame, source_system: str, env: str) -> DataFrame:
    """
    Append standard audit/metadata columns required by the bronze layer.

    Columns added:
      _ingested_at   : ingestion timestamp (UTC)
      _source_system : originating system
      _env           : deployment environment
      _row_hash      : SHA-256 of all business column values (for dedup)
    """
    cols_for_hash = [c for c in df.columns if not c.startswith("_")]
    hash_expr = F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in cols_for_hash]), 256)

    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_system", F.lit(source_system))
        .withColumn("_env", F.lit(env))
        .withColumn("_row_hash", hash_expr)
    )


# ── Write helpers ─────────────────────────────────────────────────────────────

def write_delta_table(
    df: DataFrame,
    full_table_name: str,
    mode: str = "append",
    merge_schema: bool = True,
    partition_by: Optional[List[str]] = None,
    z_order_by: Optional[List[str]] = None,
) -> None:
    """
    Write *df* to a Delta table, optionally with ZORDER optimisation.

    Parameters
    ----------
    df              : DataFrame to write
    full_table_name : Three-part table name catalog.schema.table
    mode            : overwrite | append | merge (merge requires caller to handle)
    merge_schema    : Enable schema evolution
    partition_by    : Partition columns
    z_order_by      : Columns to ZORDER BY after write (for large tables)
    """
    writer = (
        df.write.format("delta")
        .mode(mode)
        .option("mergeSchema", str(merge_schema).lower())
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)

    logger.info("Writing %d rows to %s (mode=%s)", df.count(), full_table_name, mode)
    writer.saveAsTable(full_table_name)

    if z_order_by:
        spark = df.sparkSession
        cols = ", ".join(z_order_by)
        logger.info("Running OPTIMIZE + ZORDER BY (%s) on %s", cols, full_table_name)
        spark.sql(f"OPTIMIZE {full_table_name} ZORDER BY ({cols})")


# ── Audit helpers ─────────────────────────────────────────────────────────────

def write_audit_log(
    spark: SparkSession,
    full_table_name: str,
    layer: str,
    source_table: str,
    target_table: str,
    records_read: int,
    records_written: int,
    status: str,
    error_message: str = "",
) -> None:
    """
    Write one audit record to the pipeline audit log table.
    The table is created automatically on first write.
    """
    record = {
        "log_timestamp": datetime.now(timezone.utc).isoformat(),
        "layer": layer,
        "source_table": source_table,
        "target_table": target_table,
        "records_read": records_read,
        "records_written": records_written,
        "status": status,
        "error_message": error_message,
    }
    df = spark.createDataFrame([record])
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        full_table_name
    )
    logger.info("Audit record written to %s — status=%s", full_table_name, status)


# ── Data quality helpers ──────────────────────────────────────────────────────

def compute_null_percentages(df: DataFrame) -> Dict[str, float]:
    """Return dict of column → % null values (0.0–100.0)."""
    total = df.count()
    if total == 0:
        return {c: 0.0 for c in df.columns}
    null_counts = df.select(
        [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]
    ).collect()[0].asDict()
    return {col: round(cnt / total * 100, 2) for col, cnt in null_counts.items()}


def generate_row_hash(columns: List[str]) -> F.Column:
    """Return a PySpark column expression for a SHA-256 row hash."""
    return F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in columns]), 256)
