"""Unit tests for helper functions (PySpark-based)."""
import sys
import pytest
sys.path.insert(0, "src")
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from src.utils.helpers import (
    get_full_table_name,
    validate_dataframe_schema,
    add_metadata_columns,
    compute_null_percentages,
    generate_row_hash,
)


class TestGetFullTableName:
    def test_three_part_name(self):
        result = get_full_table_name("catalog", "schema", "table")
        assert result == "`catalog`.`schema`.`table`"

    def test_special_chars_wrapped(self):
        result = get_full_table_name("my-catalog", "my_schema", "my_table")
        assert "`my-catalog`" in result


class TestValidateDataframeSchema:

    def test_valid_schema(self, spark):
        df = spark.createDataFrame(
            [("a", 1)], schema=StructType([
                StructField("col1", StringType()),
                StructField("col2", IntegerType()),
            ])
        )
        result = validate_dataframe_schema(df, ["col1", "col2"])
        assert result["valid"] is True
        assert result["missing_columns"] == []

    def test_missing_columns_raises(self, spark):
        df = spark.createDataFrame([("a",)], schema=StructType([StructField("col1", StringType())]))
        with pytest.raises(ValueError, match="missing required columns"):
            validate_dataframe_schema(df, ["col1", "col2"], raise_on_missing=True)

    def test_missing_columns_no_raise(self, spark):
        df = spark.createDataFrame([("a",)], schema=StructType([StructField("col1", StringType())]))
        result = validate_dataframe_schema(df, ["col1", "col2"], raise_on_missing=False)
        assert result["valid"] is False
        assert "col2" in result["missing_columns"]

    def test_extra_columns_reported(self, spark):
        df = spark.createDataFrame(
            [("a", 1, "extra")],
            schema=StructType([
                StructField("col1", StringType()),
                StructField("col2", IntegerType()),
                StructField("extra_col", StringType()),
            ])
        )
        result = validate_dataframe_schema(df, ["col1", "col2"])
        assert "extra_col" in result["extra_columns"]


class TestAddMetadataColumns:

    def test_metadata_columns_added(self, spark):
        df = spark.createDataFrame(
            [("O001", "C1")],
            schema=StructType([
                StructField("order_id",    StringType()),
                StructField("customer_id", StringType()),
            ])
        )
        result = add_metadata_columns(df, "ecommerce", "test")
        assert "_ingested_at"   in result.columns
        assert "_source_system" in result.columns
        assert "_env"           in result.columns
        assert "_row_hash"      in result.columns

    def test_metadata_values(self, spark):
        df = spark.createDataFrame(
            [("O001",)], schema=StructType([StructField("order_id", StringType())])
        )
        result = add_metadata_columns(df, "ecommerce", "dev")
        row = result.collect()[0]
        assert row["_source_system"] == "ecommerce"
        assert row["_env"]           == "dev"
        assert len(row["_row_hash"]) == 64   # SHA-256 hex = 64 chars

    def test_row_hash_is_deterministic(self, spark):
        df = spark.createDataFrame(
            [("O001", "C1"), ("O001", "C1")],
            schema=StructType([
                StructField("order_id",    StringType()),
                StructField("customer_id", StringType()),
            ])
        )
        result = add_metadata_columns(df, "ecommerce", "test")
        hashes = [r["_row_hash"] for r in result.collect()]
        assert hashes[0] == hashes[1]   # identical rows → identical hashes


class TestComputeNullPercentages:

    def test_no_nulls(self, spark):
        df = spark.createDataFrame(
            [("a", 1), ("b", 2)],
            schema=StructType([StructField("c1", StringType()), StructField("c2", IntegerType())])
        )
        result = compute_null_percentages(df)
        assert result == {"c1": 0.0, "c2": 0.0}

    def test_with_nulls(self, spark):
        df = spark.createDataFrame(
            [(None, 1), ("b", 2)],
            schema=StructType([StructField("c1", StringType()), StructField("c2", IntegerType())])
        )
        result = compute_null_percentages(df)
        assert result["c1"] == 50.0
        assert result["c2"] == 0.0

    def test_empty_dataframe(self, spark):
        schema = StructType([StructField("col1", StringType())])
        df = spark.createDataFrame([], schema=schema)
        result = compute_null_percentages(df)
        assert result == {"col1": 0.0}
