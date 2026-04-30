# tests/unit/test_bronze.py
"""
Unit tests for Bronze layer ingestion logic.
Tests the transformation functions used in bronze_pipeline.py
without requiring a real DLT runtime.
"""
from __future__ import annotations

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))


# ─────────────────────────────────────────────────────────────────────────────
# Helper: reusable bronze transformation (mirrors what DLT notebook does)
# ─────────────────────────────────────────────────────────────────────────────

def apply_bronze_order_expectations(df):
    """Return rows that pass bronze expectations (order_id & customer_id not null)."""
    return df.filter(
        F.col("order_id").isNotNull() & F.col("customer_id").isNotNull()
    )


def apply_bronze_quarantine(df):
    """Return rows that FAIL bronze expectations → quarantine."""
    return df.filter(
        F.col("order_id").isNull() | F.col("customer_id").isNull()
    ).withColumn("_quarantine_reason", F.lit("null key field"))


def apply_bronze_customer_email_check(df):
    """Filter customers whose email contains '@'."""
    return df.filter(F.col("email").contains("@"))


def enrich_with_ingestion_metadata(df, env: str = "test"):
    return (
        df
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_env",       F.lit(env))
    )


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestBronzeOrderExpectations:

    def test_good_rows_pass(self, raw_orders):
        result = apply_bronze_order_expectations(raw_orders)
        assert result.count() == 3   # 5 rows, 2 have null keys

    def test_bad_rows_filtered(self, raw_orders):
        result = apply_bronze_order_expectations(raw_orders)
        assert result.filter(F.col("order_id").isNull()).count() == 0
        assert result.filter(F.col("customer_id").isNull()).count() == 0

    def test_all_valid_order_ids_present(self, raw_orders):
        result = apply_bronze_order_expectations(raw_orders)
        ids = {r["order_id"] for r in result.collect()}
        assert "ORD001" in ids
        assert "ORD002" in ids
        assert "ORD003" in ids


class TestBronzeOrderQuarantine:

    def test_quarantine_captures_bad_rows(self, raw_orders):
        result = apply_bronze_quarantine(raw_orders)
        assert result.count() == 2   # null order_id + null customer_id

    def test_quarantine_reason_set(self, raw_orders):
        result = apply_bronze_quarantine(raw_orders)
        reasons = {r["_quarantine_reason"] for r in result.collect()}
        assert reasons == {"null key field"}

    def test_no_overlap_with_good(self, raw_orders):
        good = apply_bronze_order_expectations(raw_orders)
        bad  = apply_bronze_quarantine(raw_orders)
        assert good.count() + bad.count() == raw_orders.count()


class TestBronzeCustomerExpectations:

    def test_valid_emails_pass(self, raw_customers):
        result = apply_bronze_customer_email_check(raw_customers)
        assert result.count() == 3  # one row has no @

    def test_invalid_email_excluded(self, raw_customers):
        result = apply_bronze_customer_email_check(raw_customers)
        emails = [r["email"] for r in result.collect()]
        assert "dave_noat_sign" not in emails


class TestBronzeProductExpectations:

    def test_price_filter(self, raw_products):
        result = raw_products.filter(F.col("base_price") > 0)
        assert result.count() == 3   # P004 has price=0

    def test_product_ids_not_null(self, raw_products):
        result = raw_products.filter(F.col("product_id").isNotNull())
        assert result.count() == raw_products.count()


class TestIngestionMetadata:

    def test_env_column_set(self, raw_orders):
        result = enrich_with_ingestion_metadata(raw_orders, env="test")
        assert result.filter(F.col("_env") == "test").count() == raw_orders.count()

    def test_ingest_ts_not_null(self, raw_orders):
        result = enrich_with_ingestion_metadata(raw_orders)
        assert result.filter(F.col("_ingest_ts").isNull()).count() == 0
