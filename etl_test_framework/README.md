# ETL Test Automation Framework

## Enterprise-Grade | Python + Pytest + Pandas

---

## Framework Architecture

```
etl_test_framework/
├── config/                         # Environment & DB configs
│   ├── config.yaml                 # Central config file
│   └── env_loader.py               # Config loader utility
├── connectors/                     # DB & file source connectors
│   ├── mysql_connector.py          # MySQL source/target connector
│   └── csv_connector.py            # CSV file connector
├── validators/                     # Core validation engine
│   ├── data_quality_validator.py   # Nulls, duplicates, formats
│   ├── completeness_validator.py   # Row counts, column coverage
│   ├── schema_validator.py         # Schema/DDL validations
│   └── transformation_validator.py # Business rule validations
├── tests/
│   ├── test_data_quality/          # DQ test suites
│   ├── test_transformations/       # Transformation test suites
│   ├── test_schema/                # Schema test suites
│   └── test_completeness/          # Count/completeness suites
├── fixtures/                       # Pytest fixtures (shared)
│   └── conftest.py
├── utils/
│   ├── logger.py                   # Centralized logging
│   ├── reporter.py                 # HTML/JSON report generator
│   └── helpers.py                  # Reusable helper functions
├── reports/                        # Auto-generated test reports
├── conftest.py                     # Root conftest
├── pytest.ini                      # Pytest configuration
└── requirements.txt
```

---

## Quick Start

```bash
pip install -r requirements.txt
pytest tests/ -v --html=reports/etl_report.html
```

## Run Specific Suites

```bash
# Data Quality only
pytest tests/test_data_quality/ -v -m "data_quality"

# Transformation validations
pytest tests/test_transformations/ -v -m "transformation"

# Schema checks
pytest tests/test_schema/ -v -m "schema"

# Full regression
pytest tests/ -v --tb=short --html=reports/full_report.html
```
