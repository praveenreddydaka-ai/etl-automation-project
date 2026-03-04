"""
tests/test_data_quality/test_dq_customers.py
---------------------------------------------
Data Quality tests for the Customers ETL pipeline.
Covers: nulls, duplicates, formats, ranges, referential integrity,
        allowed values, statistical comparison.
"""

import pytest
from conftest import assert_check


@pytest.mark.data_quality
@pytest.mark.critical
class TestCustomerDataQuality:
    """DQ test suite for raw_customers → dim_customers."""

    # ----------------------------------------------------------------
    # NULL CHECKS
    # ----------------------------------------------------------------

    def test_customer_id_not_null(self, target_customers_df, dq_validator):
        """customer_id must NEVER be null."""
        result = dq_validator.check_not_null_columns(
            target_customers_df,
            columns=["customer_id"]
        )
        assert_check(result)

    def test_mandatory_fields_not_null(self, target_customers_df, dq_validator):
        """Business-critical fields must have zero nulls."""
        result = dq_validator.check_not_null_columns(
            target_customers_df,
            columns=["customer_id", "first_name", "last_name", "email", "created_at"]
        )
        assert_check(result)

    def test_optional_fields_null_threshold(self, target_customers_df, dq_validator):
        """Optional fields may have nulls but within allowed threshold."""
        result = dq_validator.check_nulls(
            target_customers_df,
            columns=["phone", "address_line2", "middle_name"],
            max_null_pct=30.0  # 30% allowed for optional fields
        )
        assert_check(result)

    def test_all_columns_null_threshold(self, target_customers_df, dq_validator):
        """All columns must stay within the global null threshold."""
        result = dq_validator.check_nulls(target_customers_df)
        assert_check(result)

    # ----------------------------------------------------------------
    # DUPLICATE CHECKS
    # ----------------------------------------------------------------

    def test_no_duplicate_customer_ids(self, target_customers_df, dq_validator):
        """customer_id must be globally unique."""
        result = dq_validator.check_duplicates(
            target_customers_df,
            key_columns=["customer_id"]
        )
        assert_check(result)

    def test_no_duplicate_email_addresses(self, target_customers_df, dq_validator):
        """Email addresses must be unique per customer."""
        result = dq_validator.check_duplicates(
            target_customers_df,
            key_columns=["email"]
        )
        assert_check(result)

    # ----------------------------------------------------------------
    # FORMAT CHECKS
    # ----------------------------------------------------------------

    def test_email_format_valid(self, target_customers_df, dq_validator):
        """All email addresses must be in valid format."""
        result = dq_validator.check_column_format(
            target_customers_df,
            column="email",
            pattern_name="email"
        )
        assert_check(result)

    def test_phone_format_valid(self, target_customers_df, dq_validator):
        """Phone numbers must match E.164 pattern."""
        result = dq_validator.check_column_format(
            target_customers_df.dropna(subset=["phone"]),
            column="phone",
            pattern_name="phone_us"
        )
        assert_check(result)

    def test_customer_id_format(self, target_customers_df, dq_validator):
        """customer_id must follow CUST-XXXXXXXX format."""
        result = dq_validator.check_column_format(
            target_customers_df,
            column="customer_id",
            custom_pattern=r"^CUST-\d{8}$"
        )
        assert_check(result)

    # ----------------------------------------------------------------
    # DATA TYPE CHECKS
    # ----------------------------------------------------------------

    def test_column_data_types(self, target_customers_df, dq_validator):
        """Assert correct data types for key columns."""
        result = dq_validator.check_data_types(
            target_customers_df,
            expected_dtypes={
                "customer_id": "object",
                "age": "int64",
                "credit_score": "float64",
                "is_active": "bool",
                "email": "object",
            }
        )
        assert_check(result)

    # ----------------------------------------------------------------
    # RANGE / VALUE CHECKS
    # ----------------------------------------------------------------

    def test_age_range(self, target_customers_df, dq_validator):
        """Customer age must be between 18 and 120."""
        result = dq_validator.check_value_range(
            target_customers_df.dropna(subset=["age"]),
            column="age",
            min_val=18,
            max_val=120
        )
        assert_check(result)

    def test_credit_score_range(self, target_customers_df, dq_validator):
        """Credit score must be between 300 and 850."""
        result = dq_validator.check_value_range(
            target_customers_df.dropna(subset=["credit_score"]),
            column="credit_score",
            min_val=300,
            max_val=850
        )
        assert_check(result)

    def test_customer_status_allowed_values(self, target_customers_df, dq_validator):
        """Status must be one of the defined business values."""
        result = dq_validator.check_allowed_values(
            target_customers_df,
            column="status",
            allowed_values=["ACTIVE", "INACTIVE", "SUSPENDED", "PENDING"],
            case_sensitive=False
        )
        assert_check(result)

    def test_country_code_allowed_values(self, target_customers_df, dq_validator):
        """Country codes must be valid ISO 3166-1 alpha-2."""
        result = dq_validator.check_allowed_values(
            target_customers_df,
            column="country_code",
            allowed_values=["US", "CA", "GB", "AU", "DE", "FR", "IN"]
        )
        assert_check(result)

    # ----------------------------------------------------------------
    # STATISTICAL COMPARISON (SOURCE VS TARGET)
    # ----------------------------------------------------------------

    def test_credit_score_statistics(
        self, source_customers_df, target_customers_df, dq_validator
    ):
        """Statistical profile of credit_score should match between source & target."""
        result = dq_validator.compare_column_statistics(
            source_customers_df,
            target_customers_df,
            column="credit_score"
        )
        assert_check(result)

    # ----------------------------------------------------------------
    # REFERENTIAL INTEGRITY
    # ----------------------------------------------------------------

    def test_order_customer_fk_integrity(
        self, target_customers_df, target_orders_df, dq_validator
    ):
        """All orders must reference a valid customer."""
        result = dq_validator.check_referential_integrity(
            child_df=target_orders_df,
            parent_df=target_customers_df,
            child_key="customer_id",
            parent_key="customer_id"
        )
        assert_check(result)


@pytest.mark.data_quality
class TestOrderDataQuality:
    """DQ test suite for raw_orders → fact_orders."""

    def test_order_mandatory_nulls(self, target_orders_df, dq_validator):
        result = dq_validator.check_not_null_columns(
            target_orders_df,
            columns=["order_id", "customer_id", "order_date", "total_amount"]
        )
        assert_check(result)

    def test_no_duplicate_order_ids(self, target_orders_df, dq_validator):
        result = dq_validator.check_duplicates(
            target_orders_df, key_columns=["order_id"]
        )
        assert_check(result)

    def test_total_amount_positive(self, target_orders_df, dq_validator):
        """All order amounts must be positive."""
        result = dq_validator.check_value_range(
            target_orders_df,
            column="total_amount",
            min_val=0.01
        )
        assert_check(result)

    def test_order_status_values(self, target_orders_df, dq_validator):
        result = dq_validator.check_allowed_values(
            target_orders_df,
            column="order_status",
            allowed_values=["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]
        )
        assert_check(result)

    def test_order_date_format(self, target_orders_df, dq_validator):
        result = dq_validator.check_column_format(
            target_orders_df,
            column="order_date",
            pattern_name="date_iso"
        )
        assert_check(result)
