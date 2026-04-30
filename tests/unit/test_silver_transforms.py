"""Unit tests for silver layer transformation logic."""
import sys
import pytest
from datetime import date
sys.path.insert(0, "src")
from pyspark.sql import functions as F
from pyspark.sql.types import *


def _make_orders_df(spark, data):
    schema = StructType([
        StructField("order_id",      StringType()),
        StructField("customer_id",   StringType()),
        StructField("product_id",    StringType()),
        StructField("quantity",      IntegerType()),
        StructField("unit_price",    DoubleType()),
        StructField("order_date",    StringType()),
        StructField("status",        StringType()),
        StructField("store_id",      StringType()),
        StructField("_ingested_at",  TimestampType()),
        StructField("_source_system",StringType()),
        StructField("_env",          StringType()),
        StructField("_row_hash",     StringType()),
    ])
    return spark.createDataFrame(data, schema=schema)


def _apply_silver_orders_transform(df):
    """Mirror the transformation logic from silver_layer.py (testable without DLT)."""
    return (
        df
        .withColumn("order_date",    F.to_date("order_date", "yyyy-MM-dd"))
        .withColumn("quantity",      F.col("quantity").cast("integer"))
        .withColumn("unit_price",    F.col("unit_price").cast(DoubleType()))
        .withColumn("total_amount",  F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("status",        F.upper("status"))
        .withColumn("_processed_at", F.current_timestamp())
        .dropDuplicates(["order_id", "_row_hash"])
    )


class TestSilverOrdersTransform:

    def test_total_amount_calculated(self, spark, sample_orders_data):
        from datetime import datetime
        rows = [
            {**r, "_ingested_at": datetime.now(), "_source_system": "ec",
             "_env": "test", "_row_hash": f"hash{i}"}
            for i, r in enumerate(sample_orders_data)
        ]
        df = _make_orders_df(spark, rows)
        result = _apply_silver_orders_transform(df)
        row = result.filter(F.col("order_id") == "O001").collect()[0]
        assert row["total_amount"] == pytest.approx(100.0)   # 2 × 50.0

    def test_status_uppercased(self, spark, sample_orders_data):
        from datetime import datetime
        rows = [
            {**r, "_ingested_at": datetime.now(), "_source_system": "ec",
             "_env": "test", "_row_hash": f"hash{i}"}
            for i, r in enumerate(sample_orders_data)
        ]
        df = _make_orders_df(spark, rows)
        result = _apply_silver_orders_transform(df)
        statuses = {r["status"] for r in result.collect()}
        assert all(s == s.upper() for s in statuses)

    def test_order_date_is_date_type(self, spark, sample_orders_data):
        from datetime import datetime
        rows = [
            {**r, "_ingested_at": datetime.now(), "_source_system": "ec",
             "_env": "test", "_row_hash": f"hash{i}"}
            for i, r in enumerate(sample_orders_data)
        ]
        df = _make_orders_df(spark, rows)
        result = _apply_silver_orders_transform(df)
        dt = dict(result.dtypes)
        assert dt["order_date"] == "date"

    def test_deduplication_removes_duplicates(self, spark):
        from datetime import datetime
        dup_rows = [
            {"order_id": "O999", "customer_id": "C1", "product_id": "P1",
             "quantity": 1, "unit_price": 10.0, "order_date": "2024-01-01",
             "status": "PENDING", "store_id": "S1",
             "_ingested_at": datetime.now(), "_source_system": "ec",
             "_env": "test", "_row_hash": "same_hash"},
            {"order_id": "O999", "customer_id": "C1", "product_id": "P1",
             "quantity": 1, "unit_price": 10.0, "order_date": "2024-01-01",
             "status": "PENDING", "store_id": "S1",
             "_ingested_at": datetime.now(), "_source_system": "ec",
             "_env": "test", "_row_hash": "same_hash"},
        ]
        df = _make_orders_df(spark, dup_rows)
        result = _apply_silver_orders_transform(df)
        assert result.count() == 1


class TestSilverProductsTransform:

    def _apply_transform(self, df):
        return (
            df
            .withColumn("cost_price",   F.col("cost_price").cast(DoubleType()))
            .withColumn("list_price",   F.col("list_price").cast(DoubleType()))
            .withColumn("margin_amount", F.round(F.col("list_price") - F.col("cost_price"), 2))
            .withColumn("margin_pct",
                F.when(F.col("cost_price") > 0,
                    F.round((F.col("list_price") - F.col("cost_price")) / F.col("cost_price") * 100, 2)
                ).otherwise(F.lit(None))
            )
            .withColumn("category",     F.initcap("category"))
            .withColumn("sub_category", F.initcap("sub_category"))
        )

    def test_margin_calculated(self, spark, sample_products_data):
        schema = StructType([
            StructField("product_id",   StringType()),
            StructField("product_name", StringType()),
            StructField("category",     StringType()),
            StructField("sub_category", StringType()),
            StructField("cost_price",   DoubleType()),
            StructField("list_price",   DoubleType()),
            StructField("supplier_id",  StringType()),
        ])
        df = spark.createDataFrame(sample_products_data, schema)
        result = self._apply_transform(df)
        row = result.filter(F.col("product_id") == "P1").collect()[0]
        assert row["margin_amount"] == pytest.approx(25.0)   # 50 - 25
        assert row["margin_pct"]    == pytest.approx(100.0)  # 25/25 * 100

    def test_category_title_cased(self, spark, sample_products_data):
        schema = StructType([
            StructField("product_id",   StringType()),
            StructField("product_name", StringType()),
            StructField("category",     StringType()),
            StructField("sub_category", StringType()),
            StructField("cost_price",   DoubleType()),
            StructField("list_price",   DoubleType()),
            StructField("supplier_id",  StringType()),
        ])
        df = spark.createDataFrame(sample_products_data, schema)
        result = self._apply_transform(df)
        row = result.filter(F.col("product_id") == "P1").collect()[0]
        assert row["category"] == "Electronics"
