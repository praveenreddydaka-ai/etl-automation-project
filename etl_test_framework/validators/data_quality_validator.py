"""
validators/data_quality_validator.py
-------------------------------------
Core data quality validation engine covering:
  - Null / missing value analysis
  - Duplicate detection
  - Data type conformance
  - Format/pattern validation (email, phone, date)
  - Referential integrity
  - Outlier / range checks
  - Column-level statistics comparison
"""

import logging
import re
from typing import Optional

import pandas as pd
import pandas.testing as pdt

logger = logging.getLogger(__name__)


class DataQualityValidator:
    """
    Enterprise Data Quality Validator.
    All methods return a structured result dict:
        {
            "check": str,
            "passed": bool,
            "details": str,
            "data": pd.DataFrame | None
        }
    """

    def __init__(self, threshold_config: dict = None):
        self.thresholds = threshold_config or {
            "max_null_percentage": 5.0,
            "max_duplicate_percentage": 0.1,
            "numeric_tolerance": 0.001,
        }

    def _result(self, check: str, passed: bool, details: str, data=None) -> dict:
        status = "✅ PASS" if passed else "❌ FAIL"
        logger.info(f"[DQ] {status} | {check}: {details}")
        return {"check": check, "passed": passed, "details": details, "data": data}

    # ------------------------------------------------------------------
    # NULL CHECKS
    # ------------------------------------------------------------------

    def check_nulls(
        self,
        df: pd.DataFrame,
        columns: Optional[list] = None,
        max_null_pct: Optional[float] = None,
        not_null_columns: Optional[list] = None,
    ) -> dict:
        """
        Check null percentages across columns.

        Args:
            columns:          Columns to check (all if None)
            max_null_pct:     Override threshold for this check
            not_null_columns: Columns that must have ZERO nulls
        """
        cols = columns or df.columns.tolist()
        threshold = max_null_pct if max_null_pct is not None else self.thresholds["max_null_percentage"]

        null_report = pd.DataFrame({
            "column": cols,
            "null_count": [df[c].isna().sum() for c in cols],
            "null_pct": [(df[c].isna().mean() * 100).round(4) for c in cols],
            "total_rows": len(df),
        })

        failures = []

        # Check not-null columns (strictly zero nulls)
        if not_null_columns:
            for col in not_null_columns:
                if col in df.columns and df[col].isna().any():
                    cnt = df[col].isna().sum()
                    failures.append(f"[NOT NULL violated] '{col}' has {cnt} nulls")

        # Check threshold violations
        over_threshold = null_report[null_report["null_pct"] > threshold]
        if not over_threshold.empty:
            for _, row in over_threshold.iterrows():
                failures.append(
                    f"'{row['column']}' null%={row['null_pct']}% exceeds threshold {threshold}%"
                )

        passed = len(failures) == 0
        details = "All columns within null threshold" if passed else " | ".join(failures)
        return self._result("Null Check", passed, details, null_report)

    def check_not_null_columns(self, df: pd.DataFrame, columns: list) -> dict:
        """Assert specific columns have zero nulls."""
        return self.check_nulls(df, not_null_columns=columns, max_null_pct=0.0, columns=columns)

    # ------------------------------------------------------------------
    # DUPLICATE CHECKS
    # ------------------------------------------------------------------

    def check_duplicates(
        self,
        df: pd.DataFrame,
        key_columns: Optional[list] = None,
        max_dup_pct: Optional[float] = None,
    ) -> dict:
        """
        Detect duplicate rows or duplicate business keys.

        Args:
            key_columns:  Columns that form the unique key (all if None)
            max_dup_pct:  Override threshold
        """
        threshold = max_dup_pct if max_dup_pct is not None else self.thresholds["max_duplicate_percentage"]
        subset = key_columns

        dup_df = df[df.duplicated(subset=subset, keep=False)]
        dup_count = df.duplicated(subset=subset).sum()
        dup_pct = (dup_count / len(df) * 100).round(4) if len(df) > 0 else 0

        passed = dup_pct <= threshold
        details = (
            f"Duplicates: {dup_count} rows ({dup_pct}%) — threshold: {threshold}%"
            if not passed
            else f"No duplicates found (checked on: {subset or 'all columns'})"
        )
        return self._result("Duplicate Check", passed, details, dup_df if not passed else None)

    # ------------------------------------------------------------------
    # DATA TYPE CHECKS
    # ------------------------------------------------------------------

    def check_data_types(self, df: pd.DataFrame, expected_dtypes: dict) -> dict:
        """
        Validate column data types against expected schema.

        Args:
            expected_dtypes: {'column_name': 'expected_dtype_str'}
                             e.g. {'age': 'int64', 'salary': 'float64', 'name': 'object'}
        """
        mismatches = []
        for col, expected in expected_dtypes.items():
            if col not in df.columns:
                mismatches.append(f"'{col}' — MISSING from DataFrame")
                continue
            actual = str(df[col].dtype)
            # Allow flexible numeric checks
            if not self._dtype_compatible(actual, expected):
                mismatches.append(
                    f"'{col}': expected={expected}, actual={actual}"
                )

        passed = len(mismatches) == 0
        details = "All dtypes match" if passed else " | ".join(mismatches)
        return self._result("Data Type Check", passed, details)

    def _dtype_compatible(self, actual: str, expected: str) -> bool:
        """Flexible dtype comparison (int32 == int64 etc.)."""
        numeric_map = {
            "int": ["int8", "int16", "int32", "int64", "Int8", "Int16", "Int32", "Int64"],
            "float": ["float16", "float32", "float64"],
            "str": ["object", "string"],
            "bool": ["bool", "boolean"],
        }
        for group, types in numeric_map.items():
            if expected in types and actual in types:
                return True
        return actual == expected

    # ------------------------------------------------------------------
    # FORMAT / PATTERN CHECKS
    # ------------------------------------------------------------------

    PATTERNS = {
        "email": r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
        "phone_us": r"^\+?1?\d{10,15}$",
        "date_iso": r"^\d{4}-\d{2}-\d{2}$",
        "datetime_iso": r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}",
        "zip_us": r"^\d{5}(-\d{4})?$",
        "ssn": r"^\d{3}-\d{2}-\d{4}$",
    }

    def check_column_format(
        self,
        df: pd.DataFrame,
        column: str,
        pattern_name: str = None,
        custom_pattern: str = None,
    ) -> dict:
        """
        Validate column values against regex pattern.

        Args:
            pattern_name:   One of PATTERNS keys ('email', 'phone_us', etc.)
            custom_pattern: Custom regex string
        """
        pattern = custom_pattern or self.PATTERNS.get(pattern_name)
        if not pattern:
            raise ValueError(f"Unknown pattern '{pattern_name}'. Use: {list(self.PATTERNS.keys())}")

        col_data = df[column].dropna().astype(str)
        invalid_mask = ~col_data.str.match(pattern, na=False)
        invalid_rows = df.loc[col_data[invalid_mask].index]
        invalid_count = len(invalid_rows)

        passed = invalid_count == 0
        details = (
            f"All {len(col_data)} values match pattern"
            if passed
            else f"{invalid_count} values in '{column}' fail pattern '{pattern_name or 'custom'}'"
        )
        return self._result(
            f"Format Check [{column}]", passed, details,
            invalid_rows if not passed else None
        )

    # ------------------------------------------------------------------
    # RANGE / BOUNDARY CHECKS
    # ------------------------------------------------------------------

    def check_value_range(
        self,
        df: pd.DataFrame,
        column: str,
        min_val=None,
        max_val=None,
        inclusive: bool = True,
    ) -> dict:
        """Assert numeric column values are within expected range."""
        col = df[column].dropna()
        violations = pd.Series([False] * len(col), index=col.index)

        if min_val is not None:
            violations |= (col < min_val) if inclusive else (col <= min_val)
        if max_val is not None:
            violations |= (col > max_val) if inclusive else (col >= max_val)

        invalid_rows = df.loc[col[violations].index]
        passed = len(invalid_rows) == 0
        details = (
            f"All values in '{column}' within [{min_val}, {max_val}]"
            if passed
            else f"{len(invalid_rows)} values outside range [{min_val}, {max_val}]"
        )
        return self._result(f"Range Check [{column}]", passed, details, invalid_rows or None)

    # ------------------------------------------------------------------
    # ALLOWED VALUES / CATEGORICAL CHECKS
    # ------------------------------------------------------------------

    def check_allowed_values(
        self,
        df: pd.DataFrame,
        column: str,
        allowed_values: list,
        case_sensitive: bool = True,
    ) -> dict:
        """Assert a column contains only allowed/expected values."""
        col = df[column].dropna()
        if not case_sensitive:
            col = col.str.upper()
            allowed_values = [str(v).upper() for v in allowed_values]

        invalid = col[~col.isin(allowed_values)]
        passed = len(invalid) == 0
        details = (
            f"All values in '{column}' are valid"
            if passed
            else f"{len(invalid)} invalid values: {invalid.unique()[:10].tolist()}"
        )
        return self._result(f"Allowed Values [{column}]", passed, details)

    # ------------------------------------------------------------------
    # STATISTICAL / AGGREGATE COMPARISON
    # ------------------------------------------------------------------

    def compare_column_statistics(
        self,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame,
        column: str,
        tolerance: Optional[float] = None,
    ) -> dict:
        """Compare statistical profile of a column between source and target."""
        tol = tolerance or self.thresholds["numeric_tolerance"]
        src_stats = source_df[column].describe()
        tgt_stats = target_df[column].describe()

        mismatches = []
        for stat in ["mean", "std", "min", "max", "50%"]:
            s_val = src_stats.get(stat, None)
            t_val = tgt_stats.get(stat, None)
            if s_val is not None and t_val is not None:
                if abs(s_val - t_val) > tol:
                    mismatches.append(
                        f"{stat}: source={s_val:.4f} vs target={t_val:.4f}"
                    )

        passed = len(mismatches) == 0
        details = "Statistics match within tolerance" if passed else " | ".join(mismatches)
        return self._result(f"Statistical Comparison [{column}]", passed, details)

    # ------------------------------------------------------------------
    # REFERENTIAL INTEGRITY
    # ------------------------------------------------------------------

    def check_referential_integrity(
        self,
        child_df: pd.DataFrame,
        parent_df: pd.DataFrame,
        child_key: str,
        parent_key: str,
    ) -> dict:
        """
        Check all FK values in child_df exist in parent_df.

        e.g. All order.customer_id must exist in customer.customer_id
        """
        child_keys = set(child_df[child_key].dropna().unique())
        parent_keys = set(parent_df[parent_key].dropna().unique())
        orphans = child_keys - parent_keys

        passed = len(orphans) == 0
        details = (
            f"All {len(child_keys)} FK values found in parent"
            if passed
            else f"{len(orphans)} orphan FK values: {list(orphans)[:10]}"
        )
        return self._result("Referential Integrity Check", passed, details)

    # ------------------------------------------------------------------
    # PANDAS.TESTING EXACT COMPARISON
    # ------------------------------------------------------------------

    def assert_frames_equal(
        self,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame,
        check_dtype: bool = False,
        check_like: bool = True,
        rtol: float = 1e-3,
    ) -> dict:
        """
        Use pandas.testing.assert_frame_equal for exact DataFrame comparison.
        check_like=True ignores column order.
        """
        try:
            pdt.assert_frame_equal(
                source_df.reset_index(drop=True),
                target_df.reset_index(drop=True),
                check_dtype=check_dtype,
                check_like=check_like,
                rtol=rtol,
            )
            return self._result("DataFrame Equality Check", True, "DataFrames are equal")
        except AssertionError as e:
            return self._result("DataFrame Equality Check", False, str(e)[:500])
