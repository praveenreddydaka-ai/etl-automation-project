"""
conftest.py (Root)
-------------------
Pytest fixtures shared across all test modules.
Provides: DB connectors, validators, DataFrames, config.
"""

import logging
import pytest
import pandas as pd

from config.env_loader import CONFIG
from connectors.mysql_connector import MySQLConnector
from connectors.csv_connector import CSVConnector
from validators.data_quality_validator import DataQualityValidator
from validators.completeness_validator import CompletenessValidator
from validators.schema_validator import SchemaValidator
from validators.transformation_validator import TransformationValidator
from utils.logger import setup_logger
from utils.reporter import ETLTestReporter

# ─── Logger ─────────────────────────────────────────────────────────────────
setup_logger()
logger = logging.getLogger(__name__)


# ─── Config ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def etl_config():
    """Return the loaded ETL configuration."""
    return CONFIG


@pytest.fixture(scope="session")
def thresholds(etl_config):
    return etl_config.get("thresholds", {})


# ─── DB Connectors ───────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def source_db(etl_config):
    """Source MySQL DB connector (session-scoped for efficiency)."""
    connector = MySQLConnector(etl_config["source_db"], label="SOURCE")
    yield connector
    connector.dispose()


@pytest.fixture(scope="session")
def target_db(etl_config):
    """Target MySQL DB connector (session-scoped)."""
    connector = MySQLConnector(etl_config["target_db"], label="TARGET")
    yield connector
    connector.dispose()


# ─── CSV Connectors ──────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def csv_sources(etl_config):
    """Dict of CSV connectors keyed by name."""
    return {
        name: CSVConnector(path, label=name)
        for name, path in etl_config.get("csv_sources", {}).items()
    }


@pytest.fixture(scope="session")
def customers_csv(csv_sources):
    return csv_sources["customers"].load()


@pytest.fixture(scope="session")
def orders_csv(csv_sources):
    return csv_sources["orders"].load()


# ─── Validators ──────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def dq_validator(thresholds):
    return DataQualityValidator(threshold_config=thresholds)


@pytest.fixture(scope="session")
def completeness_validator(thresholds):
    return CompletenessValidator(
        tolerance_pct=thresholds.get("row_count_tolerance_pct", 0.0)
    )


@pytest.fixture(scope="session")
def schema_validator():
    return SchemaValidator()


@pytest.fixture(scope="session")
def transform_validator(thresholds):
    return TransformationValidator(
        numeric_tolerance=thresholds.get("numeric_tolerance", 0.001)
    )


# ─── Shared DataFrames ────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def source_customers_df(source_db):
    """Load full customers table from source."""
    return source_db.get_table_df("raw_customers")


@pytest.fixture(scope="session")
def target_customers_df(target_db):
    """Load full customers table from target."""
    return target_db.get_table_df("dim_customers")


@pytest.fixture(scope="session")
def source_orders_df(source_db):
    return source_db.get_table_df("raw_orders")


@pytest.fixture(scope="session")
def target_orders_df(target_db):
    return target_db.get_table_df("fact_orders")


# ─── Reporter ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def reporter():
    return ETLTestReporter()


# ─── Helpers ──────────────────────────────────────────────────────────────────

def assert_check(result: dict):
    """
    Standard assertion helper for validator results.
    Raises AssertionError with detailed message on failure.
    """
    assert result["passed"], (
        f"\n{'=' * 60}"
        f"\n❌ CHECK FAILED: {result['check']}"
        f"\n   Details: {result['details']}"
        f"\n{'=' * 60}"
    )


# Attach assert_check as a built-in pytest fixture for convenience
@pytest.fixture
def assert_etl_check():
    return assert_check
