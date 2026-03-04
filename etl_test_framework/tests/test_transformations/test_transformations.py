"""
tests/test_transformations/test_transformations.py
----------------------------------------------------
ETL business transformation validation tests:
  - Derived column calculations
  - Aggregation / rollup correctness
  - Date transformations
  - String normalization
  - Status mapping / lookup
  - Conditional logic (CASE WHEN)
  - Financial calculations with penny tolerance
  - PII masking verification
"""

import pytest
import pandas as pd
from conftest import assert_check


@pytest.mark.transformation
class TestOrderTransformations:
    """Validate business transformations on fact_orders."""

    def test_net_amount_calculation(self, target_orders_df, transform_validator):
        """
        net_amount = total_amount - discount_amount + tax_amount
        """
        result = transform_validator.check_derived_column(
            df=target_orders_df,
            derived_col="net_amount",
            rule_fn=lambda df: df["total_amount"] - df["discount_amount"] + df["tax_amount"],
            tolerance=0.01  # penny tolerance
        )
        assert_check(result)

    def test_discount_percentage_column(self, target_orders_df, transform_validator):
        """
        discount_pct = (discount_amount / total_amount) * 100
        """
        result = transform_validator.check_derived_column(
            df=target_orders_df.query("total_amount > 0"),
            derived_col="discount_pct",
            rule_fn=lambda df: (df["discount_amount"] / df["total_amount"] * 100).round(2)
        )
        assert_check(result)

    def test_order_year_extraction(self, target_orders_df, transform_validator):
        """order_year must be correctly extracted from order_date."""
        result = transform_validator.check_date_transformation(
            df=target_orders_df,
            source_col="order_date",
            target_col="order_year",
            extract_part="year"
        )
        assert_check(result)

    def test_order_quarter_extraction(self, target_orders_df, transform_validator):
        """order_quarter must match the fiscal quarter of order_date."""
        result = transform_validator.check_date_transformation(
            df=target_orders_df,
            source_col="order_date",
            target_col="order_quarter",
            extract_part="quarter"
        )
        assert_check(result)

    def test_order_status_mapping(self, target_orders_df, transform_validator):
        """Raw numeric status codes must be mapped to display labels."""
        result = transform_validator.check_lookup_mapping(
            df=target_orders_df,
            source_col="raw_status_code",
            target_col="order_status",
            mapping={
                1: "PENDING",
                2: "CONFIRMED",
                3: "SHIPPED",
                4: "DELIVERED",
                9: "CANCELLED",
            },
            default_value="UNKNOWN"
        )
        assert_check(result)

    def test_order_priority_conditional(self, target_orders_df, transform_validator):
        """
        order_priority derived via CASE logic:
          total_amount >= 1000  → 'HIGH'
          total_amount >= 500   → 'MEDIUM'
          else                  → 'LOW'
        """
        result = transform_validator.check_conditional_logic(
            df=target_orders_df,
            target_col="order_priority",
            conditions=[
                (lambda df: df["total_amount"] >= 1000, "HIGH"),
                (lambda df: (df["total_amount"] >= 500) & (df["total_amount"] < 1000), "MEDIUM"),
                (lambda df: df["total_amount"] < 500, "LOW"),
            ]
        )
        assert_check(result)

    def test_revenue_aggregation_by_region(
        self, source_orders_df, target_orders_df, transform_validator
    ):
        """
        Validate SUM(total_amount) by region is consistent between
        raw source and the aggregated fact table.
        """
        result = transform_validator.check_aggregation(
            source_df=source_orders_df,
            target_df=target_orders_df,
            group_by=["region"],
            agg_column="total_amount",
            agg_func="sum",
            tolerance=0.01
        )
        assert_check(result)

    def test_revenue_aggregation_by_customer(
        self, source_orders_df, target_orders_df, transform_validator
    ):
        """Total spend per customer must match between source and target."""
        result = transform_validator.check_aggregation(
            source_df=source_orders_df,
            target_df=target_orders_df,
            group_by=["customer_id"],
            agg_column="total_amount",
            agg_func="sum",
            tolerance=0.01
        )
        assert_check(result)


@pytest.mark.transformation
class TestCustomerTransformations:
    """Validate customer data transformations."""

    def test_full_name_concatenation(self, target_customers_df, transform_validator):
        """full_name = first_name + ' ' + last_name"""
        result = transform_validator.check_derived_column(
            df=target_customers_df,
            derived_col="full_name",
            rule_fn=lambda df: df["first_name"] + " " + df["last_name"]
        )
        assert_check(result)

    def test_email_lowercased(self, target_customers_df, transform_validator):
        """Email must be stored in lowercase."""
        result = transform_validator.check_string_transformation(
            df=target_customers_df,
            source_col="raw_email",
            target_col="email",
            transform="lower"
        )
        assert_check(result)

    def test_first_name_title_case(self, target_customers_df, transform_validator):
        """First and last name must be in Title Case."""
        result = transform_validator.check_string_transformation(
            df=target_customers_df,
            source_col="raw_first_name",
            target_col="first_name",
            transform="title"
        )
        assert_check(result)

    def test_customer_segment_mapping(self, target_customers_df, transform_validator):
        """Customer segment must be mapped from credit_score ranges."""
        # This tests the segment derived from credit score bucket
        result = transform_validator.check_conditional_logic(
            df=target_customers_df.dropna(subset=["credit_score"]),
            target_col="credit_segment",
            conditions=[
                (lambda df: df["credit_score"] >= 750, "PRIME"),
                (lambda df: (df["credit_score"] >= 650) & (df["credit_score"] < 750), "NEAR_PRIME"),
                (lambda df: df["credit_score"] < 650, "SUBPRIME"),
            ]
        )
        assert_check(result)


@pytest.mark.transformation
class TestPIIMasking:
    """Validate PII masking in the output tables."""

    def test_ssn_masked_in_target(self, target_customers_df, transform_validator):
        """SSN must be masked/redacted in the target DWH table."""
        result = transform_validator.check_pii_masked(
            df=target_customers_df,
            pii_columns=["ssn"],
            mask_pattern=r"^XXX-XX-\d{4}$|REDACTED"
        )
        assert_check(result)

    def test_credit_card_not_present(self, target_customers_df, dq_validator):
        """Credit card numbers must NOT exist in the target table."""
        # Ensure cc_number column doesn't exist or is fully masked
        if "cc_number" in target_customers_df.columns:
            result = transform_validator.check_pii_masked(
                df=target_customers_df,
                pii_columns=["cc_number"]
            )
            assert_check(result)
        else:
            pytest.skip("cc_number column not present in target (expected)")
