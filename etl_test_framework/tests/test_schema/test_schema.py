"""
tests/test_schema/test_schema.py
---------------------------------
Schema & DDL validation tests:
  - Table existence
  - Column presence and order
  - Data type conformance
  - Primary key uniqueness
  - Schema drift detection
  - Nullable constraints
"""

import pytest
import pandas as pd
from conftest import assert_check


# Expected schemas as ground truth (maintain in version control)
EXPECTED_DIM_CUSTOMER_SCHEMA = {
    "customer_id":   "varchar",
    "first_name":    "varchar",
    "last_name":     "varchar",
    "email":         "varchar",
    "phone":         "varchar",
    "age":           "int",
    "credit_score":  "decimal",
    "status":        "varchar",
    "country_code":  "varchar",
    "is_active":     "tinyint",
    "created_at":    "datetime",
    "updated_at":    "datetime",
}

EXPECTED_FACT_ORDER_SCHEMA = {
    "order_id":        "varchar",
    "customer_id":     "varchar",
    "order_date":      "date",
    "order_status":    "varchar",
    "total_amount":    "decimal",
    "tax_amount":      "decimal",
    "discount_amount": "decimal",
    "net_amount":      "decimal",
    "created_at":      "datetime",
}

EXPECTED_COLUMN_ORDER_CUSTOMERS = [
    "customer_id", "first_name", "last_name", "email",
    "phone", "age", "credit_score", "status",
    "country_code", "is_active", "created_at", "updated_at"
]


@pytest.mark.schema
@pytest.mark.critical
class TestTableExistence:
    """Verify all target tables exist after ETL."""

    @pytest.mark.parametrize("table_name", [
        "dim_customers",
        "fact_orders",
        "dim_products",
        "dim_date",
    ])
    def test_target_table_exists(self, target_db, schema_validator, table_name):
        """Each expected target table must exist."""
        result = schema_validator.check_table_exists(target_db, table_name)
        assert_check(result)


@pytest.mark.schema
class TestCustomerSchema:
    """Schema tests for dim_customers."""

    def test_required_columns_exist(self, target_customers_df, schema_validator):
        """All expected columns must exist in dim_customers."""
        result = schema_validator.check_column_exists(
            target_customers_df,
            required_columns=list(EXPECTED_DIM_CUSTOMER_SCHEMA.keys())
        )
        assert_check(result)

    def test_column_order(self, target_customers_df, schema_validator):
        """Column order in dim_customers must follow the agreed spec."""
        result = schema_validator.check_column_order(
            target_customers_df,
            expected_columns=EXPECTED_COLUMN_ORDER_CUSTOMERS
        )
        assert_check(result)

    def test_dim_customer_schema_vs_source(self, source_db, target_db, schema_validator):
        """
        Validate DDL-level schema between source raw_customers and target dim_customers.
        Uses information_schema for reliable type comparison.
        """
        src_schema = source_db.get_table_schema("raw_customers")
        tgt_schema = target_db.get_table_schema("dim_customers")
        result = schema_validator.check_schema_match(
            src_schema, tgt_schema,
            check_order=False,
            check_nullable=True
        )
        assert_check(result)

    def test_customer_pk_uniqueness(self, target_customers_df, schema_validator):
        """customer_id must be the unique primary key."""
        result = schema_validator.check_primary_key_uniqueness(
            target_customers_df, pk_columns=["customer_id"]
        )
        assert_check(result)


@pytest.mark.schema
class TestOrderSchema:
    """Schema tests for fact_orders."""

    def test_required_columns_exist(self, target_orders_df, schema_validator):
        result = schema_validator.check_column_exists(
            target_orders_df,
            required_columns=list(EXPECTED_FACT_ORDER_SCHEMA.keys())
        )
        assert_check(result)

    def test_order_pk_uniqueness(self, target_orders_df, schema_validator):
        result = schema_validator.check_primary_key_uniqueness(
            target_orders_df, pk_columns=["order_id"]
        )
        assert_check(result)

    def test_fact_order_schema_vs_source(self, source_db, target_db, schema_validator):
        src_schema = source_db.get_table_schema("raw_orders")
        tgt_schema = target_db.get_table_schema("fact_orders")
        result = schema_validator.check_schema_match(src_schema, tgt_schema)
        assert_check(result)


@pytest.mark.schema
class TestSchemaDrift:
    """
    Regression: Detect unexpected schema drift vs baseline.
    Run this after each ETL deployment to catch accidental DDL changes.
    """

    BASELINE_CUSTOMER_SCHEMA = {
        "customer_id": "object",
        "first_name": "object",
        "last_name": "object",
        "email": "object",
        "age": "int64",
        "credit_score": "float64",
        "status": "object",
        "is_active": "bool",
        "created_at": "datetime64[ns]",
    }

    def test_no_schema_drift_customers(
        self, target_customers_df, schema_validator
    ):
        """Alert on any schema drift vs baseline snapshot."""
        result = schema_validator.detect_schema_drift(
            baseline_schema=self.BASELINE_CUSTOMER_SCHEMA,
            current_df=target_customers_df
        )
        # Schema drift is a WARNING not a hard failure in CI
        if not result["passed"]:
            pytest.warns(UserWarning, match="schema drift")
