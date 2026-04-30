"""
Centralised pipeline configuration loaded from Databricks widgets or
environment variables. All DLT notebooks and job tasks reference this.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PipelineConfig:
    """Immutable config object built once per notebook/job execution."""

    env: str
    catalog_name: str
    schema_prefix: str
    source_system: str
    volume_name: str
    log_level: str = "INFO"
    run_date: Optional[str] = None

    # Derived attributes (auto-set in __post_init__)
    bronze_schema: str = field(init=False)
    silver_schema: str = field(init=False)
    gold_schema: str = field(init=False)

    def __post_init__(self) -> None:
        self.bronze_schema = f"{self.schema_prefix}_bronze"
        self.silver_schema = f"{self.schema_prefix}_silver"
        self.gold_schema = f"{self.schema_prefix}_gold"

    # ── Factory methods ──────────────────────────────────────────────────────

    @classmethod
    def from_dlt_context(cls) -> "PipelineConfig":
        """
        Load config from DLT pipeline configuration (set via DAB pipeline yml).
        Spark conf keys match the `configuration` block in medallion_dlt.yml.
        """
        import dlt  # noqa: F401 – only available inside DLT context
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()

        def _get(key: str, default: str = "") -> str:
            return spark.conf.get(f"pipelines.{key}", default)

        return cls(
            env=_get("env", "dev"),
            catalog_name=_get("catalog_name", "dev_catalog"),
            schema_prefix=_get("schema_prefix", "medallion"),
            source_system=_get("source_system", "ecommerce"),
            volume_name=_get("volume_name", "raw_landing"),
            log_level=_get("log_level", "INFO"),
        )

    @classmethod
    def from_widget_or_env(cls, dbutils=None) -> "PipelineConfig":
        """
        Load config from Databricks widgets (interactive) or env vars (CI/CD).
        Widget values take precedence over environment variables.
        """

        def _resolve(key: str, default: str = "") -> str:
            if dbutils is not None:
                try:
                    val = dbutils.widgets.get(key)
                    if val:
                        return val
                except Exception:
                    pass
            return os.environ.get(key.upper(), default)

        return cls(
            env=_resolve("env", "dev"),
            catalog_name=_resolve("catalog_name", "dev_catalog"),
            schema_prefix=_resolve("schema_prefix", "medallion"),
            source_system=_resolve("source_system", "ecommerce"),
            volume_name=_resolve("volume_name", "raw_landing"),
            log_level=_resolve("log_level", "INFO"),
            run_date=_resolve("run_date"),
        )

    # ── Helpers ──────────────────────────────────────────────────────────────

    def full_table(self, schema: str, table: str) -> str:
        """Return three-part Unity Catalog identifier."""
        return f"{self.catalog_name}.{schema}.{table}"

    def volume_path(self, sub_path: str = "") -> str:
        """Return the Unity Catalog volume path."""
        base = f"/Volumes/{self.catalog_name}/{self.bronze_schema}/{self.volume_name}"
        return f"{base}/{sub_path}" if sub_path else base

    def is_production(self) -> bool:
        return self.env == "prod"
