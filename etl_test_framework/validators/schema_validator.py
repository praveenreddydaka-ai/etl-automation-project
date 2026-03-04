"""
validators/schema_validator.py
-------------------------------
DDL and schema validation:
  - Column existence & order
  - Data type matching
  - Nullable constraints
  - Primary/Foreign key presence
  - Table existence
  - Schema drift detection
"""

import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


class SchemaValidator:

    def __init__(self):
        pass

    def _result(self, check, passed, details, data=None):
        status = "✅ PASS" if passed else "❌ FAIL"
        logger.info(f"[SCHEMA] {status} | {check}: {details}")
        return {"check": check, "passed": passed, "details": details, "data": data}

    def check_table_exists(self, connector, table_name: str) -> dict:
        """Verify a table exists in the target database."""
        exists = connector.table_exists(table_name)
        return self._result(
            "Table Exists",
            exists,
            f"Table '{table_name}' {'found' if exists else 'NOT FOUND'}"
        )

    def check_column_exists(
        self,
        df: pd.DataFrame,
        required_columns: list,
    ) -> dict:
        """Assert all required columns exist in the DataFrame."""
        missing = [c for c in required_columns if c not in df.columns]
        passed = len(missing) == 0
        details = "All required columns present" if passed else f"Missing columns: {missing}"
        return self._result("Column Existence Check", passed, details)

    def check_schema_match(
        self,
        source_schema: pd.DataFrame,
        target_schema: pd.DataFrame,
        check_order: bool = False,
        check_nullable: bool = True,
    ) -> dict:
        """
        Compare two schema DataFrames (from MySQLConnector.get_table_schema).

        Args:
            source_schema:  Expected schema (from information_schema)
            target_schema:  Actual schema (from information_schema)
            check_order:    Whether column order must match
            check_nullable: Whether nullable property must match
        """
        src = source_schema.set_index("COLUMN_NAME")
        tgt = target_schema.set_index("COLUMN_NAME")

        src_cols = set(src.index)
        tgt_cols = set(tgt.index)

        issues = []

        # Missing columns
        missing = src_cols - tgt_cols
        extra = tgt_cols - src_cols
        if missing:
            issues.append(f"Columns missing in target: {sorted(missing)}")
        if extra:
            issues.append(f"Extra columns in target: {sorted(extra)}")

        # Type mismatches for common columns
        common = src_cols & tgt_cols
        for col in common:
            src_type = str(src.loc[col, "DATA_TYPE"]).upper()
            tgt_type = str(tgt.loc[col, "DATA_TYPE"]).upper()
            if src_type != tgt_type:
                issues.append(f"'{col}': type mismatch source={src_type} vs target={tgt_type}")

            if check_nullable:
                src_null = str(src.loc[col, "IS_NULLABLE"]).upper()
                tgt_null = str(tgt.loc[col, "IS_NULLABLE"]).upper()
                if src_null != tgt_null:
                    issues.append(f"'{col}': nullable mismatch source={src_null} vs target={tgt_null}")

        if check_order:
            src_order = source_schema["COLUMN_NAME"].tolist()
            tgt_order = [c for c in target_schema["COLUMN_NAME"].tolist() if c in src_order]
            if src_order != tgt_order:
                issues.append(f"Column order mismatch: {src_order} vs {tgt_order}")

        passed = len(issues) == 0
        details = "Schema matches" if passed else " | ".join(issues)
        return self._result("Schema Match", passed, details)

    def check_primary_key_uniqueness(
        self,
        df: pd.DataFrame,
        pk_columns: list,
    ) -> dict:
        """Assert the primary key is unique across all rows."""
        dup_mask = df.duplicated(subset=pk_columns, keep=False)
        dup_count = dup_mask.sum()
        passed = dup_count == 0
        details = (
            f"PK {pk_columns} is unique ({len(df):,} rows)"
            if passed
            else f"PK {pk_columns} has {dup_count} duplicate rows"
        )
        return self._result("PK Uniqueness", passed, details, df[dup_mask] if not passed else None)

    def check_column_order(
        self,
        df: pd.DataFrame,
        expected_columns: list,
    ) -> dict:
        """Assert columns appear in the expected order."""
        actual = df.columns.tolist()
        # Only compare columns that exist in expected
        actual_filtered = [c for c in actual if c in expected_columns]
        passed = actual_filtered == expected_columns
        details = (
            "Column order matches"
            if passed
            else f"Expected: {expected_columns} | Got: {actual_filtered}"
        )
        return self._result("Column Order Check", passed, details)

    def detect_schema_drift(
        self,
        baseline_schema: dict,
        current_df: pd.DataFrame,
    ) -> dict:
        """
        Detect schema drift between a saved baseline schema and current state.

        Args:
            baseline_schema: {'col_name': 'expected_dtype'} from previous run
            current_df:      Current DataFrame to compare
        """
        current_schema = {col: str(dtype) for col, dtype in current_df.dtypes.items()}
        added = set(current_schema) - set(baseline_schema)
        removed = set(baseline_schema) - set(current_schema)
        type_changed = {
            col: f"{baseline_schema[col]} → {current_schema[col]}"
            for col in set(baseline_schema) & set(current_schema)
            if baseline_schema[col] != current_schema[col]
        }

        drift_report = {
            "added_columns": list(added),
            "removed_columns": list(removed),
            "type_changes": type_changed,
        }

        drifts = []
        if added:
            drifts.append(f"New columns: {list(added)}")
        if removed:
            drifts.append(f"Removed columns: {list(removed)}")
        if type_changed:
            drifts.append(f"Type changes: {type_changed}")

        passed = len(drifts) == 0
        details = "No schema drift detected" if passed else " | ".join(drifts)
        return self._result("Schema Drift Detection", passed, details, drift_report)
