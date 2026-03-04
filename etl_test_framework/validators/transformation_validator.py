"""
validators/transformation_validator.py
----------------------------------------
Business logic & transformation rule validation:
  - Column derivation rules
  - Aggregation/rollup correctness
  - Conditional logic
  - Date/time transformations
  - String transformations
  - Lookup/mapping validation
  - Financial calculations
"""

import logging
from typing import Callable, Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class TransformationValidator:

    def __init__(self, numeric_tolerance: float = 0.001):
        self.tolerance = numeric_tolerance

    def _result(self, check, passed, details, data=None):
        status = "✅ PASS" if passed else "❌ FAIL"
        logger.info(f"[TRANSFORM] {status} | {check}: {details}")
        return {"check": check, "passed": passed, "details": details, "data": data}

    # ------------------------------------------------------------------
    # DERIVED COLUMN CHECKS
    # ------------------------------------------------------------------

    def check_derived_column(
        self,
        df: pd.DataFrame,
        derived_col: str,
        rule_fn: Callable[[pd.DataFrame], pd.Series],
        tolerance: Optional[float] = None,
    ) -> dict:
        """
        Assert a derived column matches the expected calculation.

        Args:
            derived_col: Column in df to validate
            rule_fn:     Function that takes the df and returns expected Series
                         e.g. lambda df: df['qty'] * df['unit_price']
        """
        tol = tolerance if tolerance is not None else self.tolerance
        expected = rule_fn(df)
        actual = df[derived_col]

        is_numeric = pd.api.types.is_numeric_dtype(expected)

        if is_numeric:
            diff = (actual - expected).abs()
            violations = diff > tol
        else:
            violations = actual != expected

        invalid_df = df[violations]
        passed = len(invalid_df) == 0
        details = (
            f"'{derived_col}' matches rule for all {len(df):,} rows"
            if passed
            else f"{violations.sum()} rows have incorrect '{derived_col}' values"
        )
        return self._result(f"Derived Column [{derived_col}]", passed, details, invalid_df or None)

    # ------------------------------------------------------------------
    # AGGREGATION CHECKS
    # ------------------------------------------------------------------

    def check_aggregation(
        self,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame,
        group_by: list,
        agg_column: str,
        agg_func: str = "sum",
        tolerance: Optional[float] = None,
    ) -> dict:
        """
        Validate aggregated values in target match aggregated source.

        e.g. target fact table total_sales per region should match
             SUM of raw_orders.amount grouped by region.
        """
        tol = tolerance if tolerance is not None else self.tolerance
        fn_map = {
            "sum": "sum", "count": "count",
            "mean": "mean", "max": "max", "min": "min"
        }
        agg_fn = fn_map.get(agg_func, "sum")

        src_agg = (
            source_df.groupby(group_by)[agg_column]
            .agg(agg_fn)
            .reset_index()
            .rename(columns={agg_column: f"src_{agg_column}"})
        )
        tgt_agg = (
            target_df.groupby(group_by)[agg_column]
            .agg(agg_fn)
            .reset_index()
            .rename(columns={agg_column: f"tgt_{agg_column}"})
        )

        merged = src_agg.merge(tgt_agg, on=group_by, how="outer", indicator=True)
        merged["diff"] = (
            merged[f"src_{agg_column}"].fillna(0) -
            merged[f"tgt_{agg_column}"].fillna(0)
        ).abs()
        violations = merged[merged["diff"] > tol]

        passed = len(violations) == 0
        details = (
            f"All {agg_func}({agg_column}) aggregations match by {group_by}"
            if passed
            else f"{len(violations)} group-by mismatches found"
        )
        return self._result(
            f"Aggregation Check [{agg_func}({agg_column}) by {group_by}]",
            passed, details, violations if not passed else None
        )

    # ------------------------------------------------------------------
    # DATE TRANSFORMATION CHECKS
    # ------------------------------------------------------------------

    def check_date_transformation(
        self,
        df: pd.DataFrame,
        source_col: str,
        target_col: str,
        expected_format: str = "%Y-%m-%d",
        extract_part: Optional[str] = None,
    ) -> dict:
        """
        Validate date transformations (format conversion, part extraction).

        Args:
            source_col:    Raw date column
            target_col:    Transformed date column
            extract_part:  One of 'year', 'month', 'day', 'quarter' (if applicable)
        """
        src_dates = pd.to_datetime(df[source_col], errors="coerce")

        if extract_part:
            part_map = {
                "year": src_dates.dt.year,
                "month": src_dates.dt.month,
                "day": src_dates.dt.day,
                "quarter": src_dates.dt.quarter,
                "weekday": src_dates.dt.dayofweek,
            }
            expected = part_map.get(extract_part)
            if expected is None:
                raise ValueError(f"Unsupported extract_part: {extract_part}")
            violations = df[df[target_col] != expected]
        else:
            expected = src_dates.dt.strftime(expected_format)
            violations = df[df[target_col].astype(str) != expected]

        passed = len(violations) == 0
        details = (
            f"Date transformation '{source_col}' → '{target_col}' is correct"
            if passed
            else f"{len(violations)} rows have incorrect date transformation"
        )
        return self._result(f"Date Transform [{source_col}→{target_col}]", passed, details, violations or None)

    # ------------------------------------------------------------------
    # STRING TRANSFORMATION CHECKS
    # ------------------------------------------------------------------

    def check_string_transformation(
        self,
        df: pd.DataFrame,
        source_col: str,
        target_col: str,
        transform: str,  # 'upper', 'lower', 'title', 'strip', 'replace'
        replace_from: str = None,
        replace_to: str = None,
    ) -> dict:
        """
        Validate string transformations.
        """
        src = df[source_col].astype(str)

        transform_map = {
            "upper": src.str.upper(),
            "lower": src.str.lower(),
            "title": src.str.title(),
            "strip": src.str.strip(),
        }

        if transform == "replace":
            expected = src.str.replace(replace_from, replace_to, regex=False)
        else:
            expected = transform_map.get(transform)
            if expected is None:
                raise ValueError(f"Unknown transform: {transform}")

        violations = df[df[target_col].astype(str) != expected]
        passed = len(violations) == 0
        details = (
            f"String transform '{transform}' on '{source_col}' → '{target_col}' is correct"
            if passed
            else f"{len(violations)} rows have incorrect string transformation"
        )
        return self._result(f"String Transform [{transform}]", passed, details, violations or None)

    # ------------------------------------------------------------------
    # LOOKUP / MAPPING CHECK
    # ------------------------------------------------------------------

    def check_lookup_mapping(
        self,
        df: pd.DataFrame,
        source_col: str,
        target_col: str,
        mapping: dict,
        default_value=None,
    ) -> dict:
        """
        Validate that source values were mapped correctly to target values.

        Args:
            mapping: {'raw_value': 'expected_mapped_value', ...}
        """
        expected = df[source_col].map(mapping).fillna(default_value)
        violations = df[df[target_col] != expected]

        passed = len(violations) == 0
        details = (
            f"Mapping for '{source_col}' → '{target_col}' is correct"
            if passed
            else f"{len(violations)} rows have incorrect mapping"
        )
        return self._result(f"Lookup Mapping [{source_col}→{target_col}]", passed, details, violations or None)

    # ------------------------------------------------------------------
    # CONDITIONAL TRANSFORMATION (CASE-WHEN LOGIC)
    # ------------------------------------------------------------------

    def check_conditional_logic(
        self,
        df: pd.DataFrame,
        target_col: str,
        conditions: list,
    ) -> dict:
        """
        Validate CASE-WHEN type conditional transformations.

        Args:
            conditions: List of tuples: (mask_fn, expected_value)
                e.g. [
                    (lambda df: df['age'] < 18, 'minor'),
                    (lambda df: df['age'] >= 18, 'adult'),
                ]
        """
        violations_total = 0
        for i, (mask_fn, expected_val) in enumerate(conditions):
            mask = mask_fn(df)
            subset = df[mask]
            wrong = subset[subset[target_col] != expected_val]
            violations_total += len(wrong)

        passed = violations_total == 0
        details = (
            f"All conditional logic for '{target_col}' is correct"
            if passed
            else f"{violations_total} rows violate conditional rules"
        )
        return self._result(f"Conditional Logic [{target_col}]", passed, details)

    # ------------------------------------------------------------------
    # FINANCIAL CALCULATIONS
    # ------------------------------------------------------------------

    def check_financial_calculation(
        self,
        df: pd.DataFrame,
        result_col: str,
        formula_fn: Callable[[pd.DataFrame], pd.Series],
        tolerance: float = 0.01,
    ) -> dict:
        """
        Validate financial calculations with penny/cent tolerance.
        """
        return self.check_derived_column(df, result_col, formula_fn, tolerance)

    # ------------------------------------------------------------------
    # DATA MASKING / PII CHECK
    # ------------------------------------------------------------------

    def check_pii_masked(
        self,
        df: pd.DataFrame,
        pii_columns: list,
        mask_pattern: str = r"^\*+$|^X+$|REDACTED|MASKED",
    ) -> dict:
        """
        Verify PII columns have been properly masked in the target.
        """
        import re
        violations = []
        for col in pii_columns:
            if col not in df.columns:
                continue
            unmasked = df[df[col].astype(str).str.match(mask_pattern) == False]
            if not unmasked.empty:
                violations.append(f"'{col}': {len(unmasked)} unmasked values")

        passed = len(violations) == 0
        details = "All PII columns are properly masked" if passed else " | ".join(violations)
        return self._result("PII Masking Check", passed, details)
