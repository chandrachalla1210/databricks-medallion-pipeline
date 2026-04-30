"""Unit tests for PipelineConfig."""
import sys
import pytest
sys.path.insert(0, "src")
from src.utils.config import PipelineConfig


class TestPipelineConfig:

    def test_derived_schema_names(self):
        cfg = PipelineConfig(
            env="dev", catalog_name="dev_catalog", schema_prefix="medallion",
            source_system="ecommerce", volume_name="raw_landing",
        )
        assert cfg.bronze_schema == "medallion_bronze"
        assert cfg.silver_schema == "medallion_silver"
        assert cfg.gold_schema   == "medallion_gold"

    def test_full_table_name(self):
        cfg = PipelineConfig(
            env="prod", catalog_name="prod_catalog", schema_prefix="medallion",
            source_system="ecommerce", volume_name="raw_landing",
        )
        result = cfg.full_table(cfg.gold_schema, "gold_daily_sales")
        assert result == "prod_catalog.medallion_gold.gold_daily_sales"

    def test_volume_path(self):
        cfg = PipelineConfig(
            env="test", catalog_name="test_catalog", schema_prefix="medallion",
            source_system="ecommerce", volume_name="raw_landing",
        )
        assert cfg.volume_path() == "/Volumes/test_catalog/medallion_bronze/raw_landing"
        assert cfg.volume_path("orders/") == "/Volumes/test_catalog/medallion_bronze/raw_landing/orders/"

    def test_is_production(self):
        prod_cfg = PipelineConfig("prod", "prod_catalog", "medallion", "ecommerce", "raw_landing")
        dev_cfg  = PipelineConfig("dev",  "dev_catalog",  "medallion", "ecommerce", "raw_landing")
        assert prod_cfg.is_production() is True
        assert dev_cfg.is_production()  is False

    def test_from_widget_or_env(self, monkeypatch):
        monkeypatch.setenv("ENV",           "int")
        monkeypatch.setenv("CATALOG_NAME",  "int_catalog")
        monkeypatch.setenv("SCHEMA_PREFIX", "medallion")
        monkeypatch.setenv("SOURCE_SYSTEM", "retail")
        monkeypatch.setenv("VOLUME_NAME",   "raw_landing")
        cfg = PipelineConfig.from_widget_or_env(dbutils=None)
        assert cfg.env == "int"
        assert cfg.catalog_name == "int_catalog"
        assert cfg.source_system == "retail"
