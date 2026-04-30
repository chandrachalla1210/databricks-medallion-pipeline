"""
Pytest fixtures shared across all unit tests.
Uses pyspark-testing (chispa) and a local SparkSession — no Databricks cluster needed.
"""
from __future__ import annotations

import os
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for the entire test session."""
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("medallion_unit_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.preview.enabled", "true")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def sample_config():
    """Return a PipelineConfig instance for tests (no Databricks required)."""
    import sys
    sys.path.insert(0, "src")
    from src.utils.config import PipelineConfig
    return PipelineConfig(
        env="test",
        catalog_name="test_catalog",
        schema_prefix="medallion",
        source_system="ecommerce",
        volume_name="raw_landing",
        log_level="DEBUG",
    )


@pytest.fixture
def sample_orders_data():
    return [
        {"order_id": "O001", "customer_id": "C1", "product_id": "P1",
         "quantity": 2, "unit_price": 50.0, "order_date": "2024-01-15",
         "status": "delivered", "store_id": "S1"},
        {"order_id": "O002", "customer_id": "C2", "product_id": "P2",
         "quantity": 1, "unit_price": 100.0, "order_date": "2024-01-16",
         "status": "pending", "store_id": "S2"},
        {"order_id": "O003", "customer_id": "C1", "product_id": "P1",
         "quantity": 3, "unit_price": 50.0, "order_date": "2024-01-17",
         "status": "cancelled", "store_id": "S1"},
    ]


@pytest.fixture
def sample_customers_data():
    return [
        {"customer_id": "C1", "first_name": "Alice", "last_name": "Smith",
         "email": "alice@example.com", "phone": "555-123-4567",
         "created_at": "2023-06-01 10:00:00", "country": "US", "segment": "premium"},
        {"customer_id": "C2", "first_name": "Bob", "last_name": "Jones",
         "email": "bob@example.com", "phone": "555-987-6543",
         "created_at": "2023-07-15 14:30:00", "country": "UK", "segment": "standard"},
    ]


@pytest.fixture
def sample_products_data():
    return [
        {"product_id": "P1", "product_name": "Widget A", "category": "electronics",
         "sub_category": "gadgets", "cost_price": 25.0, "list_price": 50.0, "supplier_id": "SUP1"},
        {"product_id": "P2", "product_name": "Widget B", "category": "electronics",
         "sub_category": "accessories", "cost_price": 60.0, "list_price": 100.0, "supplier_id": "SUP2"},
    ]
