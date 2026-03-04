"""
tests/test_completeness/test_completeness.py
---------------------------------------------
Data Completeness tests:
  - Row count reconciliation (source vs target)
  - Missing record detection
  - Column coverage
  - Aggregate sum reconciliation
"""

import pytest
from conftest import assert_check


@pytest.mark.completeness
@pytest.mark.critical
class TestCustomerCompleteness:
    """Completeness tests for customer data pipeline."""

    def test_customer_row_count_source_vs_target(
        self, source_db, target_db, completeness_validator
    ):
        """Source and target customer row counts must match exactly."""
        src_count = source_db.get_row_count("raw_customers")
        tgt_count = target_db.get_row_count("dim_customers")
        result = completeness_validator.check_row_count(
            src_count, tgt_count, label="customers"
        )
        assert_check(result)

    def test_customer_column_presence(
        self, source_customers_df, target_customers_df, completeness_validator
    ):
        """All source columns must exist in the target."""
        result = completeness_validator.check_column_count(
            source_customers_df, target_customers_df
        )
        assert_check(result)

    def test_no_missing_customer_records(
        self, source_customers_df, target_customers_df, completeness_validator
    ):
        """Every customer_id from source must appear in target."""
        result = completeness_validator.check_missing_records(
            source_customers_df, target_customers_df,
            key_column="customer_id"
        )
        assert_check(result)

    def test_no_extra_customer_records(
        self, source_customers_df, target_customers_df, completeness_validator
    ):
        """Target must not contain customer records absent from source."""
        result = completeness_validator.check_extra_records(
            source_customers_df, target_customers_df,
            key_column="customer_id"
        )
        assert_check(result)

    def test_customer_column_coverage(
        self, target_customers_df, completeness_validator
    ):
        """All core columns must have >= 95% non-null coverage."""
        result = completeness_validator.check_column_coverage(
            target_customers_df,
            min_coverage_pct=95.0,
            columns=["customer_id", "email", "first_name", "last_name", "created_at"]
        )
        assert_check(result)


@pytest.mark.completeness
@pytest.mark.critical
class TestOrderCompleteness:
    """Completeness tests for orders data pipeline."""

    def test_order_row_count(
        self, source_db, target_db, completeness_validator
    ):
        src_count = source_db.get_row_count("raw_orders")
        tgt_count = target_db.get_row_count("fact_orders")
        result = completeness_validator.check_row_count(
            src_count, tgt_count, label="orders"
        )
        assert_check(result)

    def test_no_missing_orders(
        self, source_orders_df, target_orders_df, completeness_validator
    ):
        result = completeness_validator.check_missing_records(
            source_orders_df, target_orders_df, key_column="order_id"
        )
        assert_check(result)

    def test_total_revenue_reconciliation(
        self, source_orders_df, target_orders_df, completeness_validator
    ):
        """Total revenue sum must match between source and target."""
        result = completeness_validator.check_aggregate_sums(
            source_orders_df, target_orders_df,
            numeric_columns=["total_amount", "tax_amount", "discount_amount"],
            tolerance=0.01
        )
        assert_check(result)


@pytest.mark.completeness
class TestCSVCompleteness:
    """Completeness tests for CSV → MySQL pipeline."""

    def test_csv_to_db_row_count(
        self, csv_sources, target_db, completeness_validator
    ):
        """CSV customer records must all land in target DB."""
        csv_count = csv_sources["customers"].get_row_count()
        db_count = target_db.get_row_count("dim_customers")
        result = completeness_validator.check_row_count(
            csv_count, db_count, label="customers_csv_to_db"
        )
        assert_check(result)

    def test_csv_column_coverage(self, customers_csv, completeness_validator):
        """CSV must have all required columns with sufficient data."""
        result = completeness_validator.check_column_coverage(
            customers_csv,
            min_coverage_pct=90.0,
            columns=["customer_id", "email", "first_name", "last_name"]
        )
        assert_check(result)
