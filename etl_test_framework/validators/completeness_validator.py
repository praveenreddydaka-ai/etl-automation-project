"""
validators/completeness_validator.py
--------------------------------------
Validates data completeness between source and target:
  - Row count reconciliation
  - Column completeness
  - Primary key coverage
  - Missing record detection
"""

import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


class CompletenessValidator:

    def __init__(self, tolerance_pct: float = 0.0):
        self.tolerance_pct = tolerance_pct  # 0.0 = zero tolerance

    def _result(self, check, passed, details, data=None):
        status = "✅ PASS" if passed else "❌ FAIL"
        logger.info(f"[COMPLETENESS] {status} | {check}: {details}")
        return {"check": check, "passed": passed, "details": details, "data": data}

    def check_row_count(
        self,
        source_count: int,
        target_count: int,
        label: str = "table",
    ) -> dict:
        """
        Compare source vs target row counts.
        Supports tolerance percentage for large tables.
        """
        diff = abs(source_count - target_count)
        diff_pct = (diff / source_count * 100) if source_count > 0 else 0

        passed = diff_pct <= self.tolerance_pct
        details = (
            f"[{label}] Source={source_count:,} | Target={target_count:,} | "
            f"Diff={diff:,} ({diff_pct:.4f}%) | Tolerance={self.tolerance_pct}%"
        )
        return self._result("Row Count Check", passed, details)

    def check_column_count(
        self,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame,
    ) -> dict:
        """Assert target has all expected columns from source."""
        src_cols = set(source_df.columns)
        tgt_cols = set(target_df.columns)
        missing = src_cols - tgt_cols
        extra = tgt_cols - src_cols

        passed = len(missing) == 0
        details_parts = []
        if missing:
            details_parts.append(f"Missing in target: {sorted(missing)}")
        if extra:
            details_parts.append(f"Extra in target: {sorted(extra)}")
        details = " | ".join(details_parts) if details_parts else "All expected columns present"
        return self._result("Column Count Check", passed, details)

    def check_missing_records(
        self,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame,
        key_column: str,
    ) -> dict:
        """
        Find records present in source but missing in target (by key).
        """
        source_keys = set(source_df[key_column].dropna().unique())
        target_keys = set(target_df[key_column].dropna().unique())
        missing_keys = source_keys - target_keys

        missing_df = source_df[source_df[key_column].isin(missing_keys)] if missing_keys else pd.DataFrame()

        passed = len(missing_keys) == 0
        details = (
            f"All {len(source_keys):,} source keys present in target"
            if passed
            else f"{len(missing_keys):,} source keys missing from target"
        )
        return self._result("Missing Records Check", passed, details, missing_df or None)

    def check_extra_records(
        self,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame,
        key_column: str,
    ) -> dict:
        """
        Find records in target that do NOT exist in source (unexpected records).
        """
        source_keys = set(source_df[key_column].dropna().unique())
        target_keys = set(target_df[key_column].dropna().unique())
        extra_keys = target_keys - source_keys

        extra_df = target_df[target_df[key_column].isin(extra_keys)] if extra_keys else pd.DataFrame()

        passed = len(extra_keys) == 0
        details = (
            "No extra records found in target"
            if passed
            else f"{len(extra_keys):,} extra records in target not in source"
        )
        return self._result("Extra Records Check", passed, details, extra_df or None)

    def check_column_coverage(
        self,
        df: pd.DataFrame,
        min_coverage_pct: float = 95.0,
        columns: Optional[list] = None,
    ) -> dict:
        """
        Each column must have at least min_coverage_pct% non-null values.
        """
        cols = columns or df.columns.tolist()
        coverage = (df[cols].notna().mean() * 100).round(2)
        below_threshold = coverage[coverage < min_coverage_pct]

        passed = below_threshold.empty
        if not passed:
            failures = [
                f"'{col}': {pct}% coverage (min {min_coverage_pct}%)"
                for col, pct in below_threshold.items()
            ]
            details = " | ".join(failures)
        else:
            details = f"All columns have >= {min_coverage_pct}% coverage"
        return self._result("Column Coverage Check", passed, details)

    def check_aggregate_sums(
        self,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame,
        numeric_columns: list,
        tolerance: float = 0.01,
    ) -> dict:
        """
        Compare aggregate SUM of numeric columns between source and target.
        Used for financial/measure reconciliation.
        """
        mismatches = []
        for col in numeric_columns:
            if col not in source_df.columns or col not in target_df.columns:
                mismatches.append(f"'{col}' missing in one of the DataFrames")
                continue
            src_sum = source_df[col].sum()
            tgt_sum = target_df[col].sum()
            diff = abs(src_sum - tgt_sum)
            if diff > tolerance:
                mismatches.append(
                    f"'{col}': source_sum={src_sum:.4f} vs target_sum={tgt_sum:.4f} diff={diff:.4f}"
                )

        passed = len(mismatches) == 0
        details = "All aggregate sums match" if passed else " | ".join(mismatches)
        return self._result("Aggregate Sum Check", passed, details)
