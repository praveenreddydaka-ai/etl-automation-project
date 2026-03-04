"""
utils/helpers.py
-----------------
Reusable helper utilities for ETL test framework.
"""

import hashlib
import logging
from typing import Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def normalize_dataframe(
    df: pd.DataFrame,
    sort_by: Optional[list] = None,
    reset_index: bool = True,
    strip_strings: bool = True,
) -> pd.DataFrame:
    """
    Normalize a DataFrame for consistent comparison:
    - Sorts by given columns
    - Resets index
    - Strips whitespace from string columns
    - Lowercases column names
    """
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    if strip_strings:
        str_cols = df.select_dtypes(include="object").columns
        df[str_cols] = df[str_cols].apply(lambda c: c.str.strip())

    if sort_by:
        sort_cols = [c.lower() for c in sort_by]
        df = df.sort_values(by=sort_cols).reset_index(drop=True)
    elif reset_index:
        df = df.reset_index(drop=True)

    return df


def compute_dataframe_hash(df: pd.DataFrame, columns: Optional[list] = None) -> str:
    """
    Compute a reproducible MD5 hash of a DataFrame for change detection.
    Useful for detecting if data has changed between runs.
    """
    cols = columns or df.columns.tolist()
    df_str = df[cols].to_csv(index=False)
    return hashlib.md5(df_str.encode()).hexdigest()


def get_mismatched_rows(
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
    key_column: str,
    compare_columns: Optional[list] = None,
    tolerance: float = 0.001,
) -> pd.DataFrame:
    """
    Find rows where source and target differ for given columns.
    Returns a DataFrame of mismatched rows with source/target values side-by-side.
    """
    compare_cols = compare_columns or [
        c for c in source_df.columns if c != key_column and c in target_df.columns
    ]
    merged = source_df.merge(
        target_df, on=key_column, suffixes=("_src", "_tgt"), how="inner"
    )

    mismatch_mask = pd.Series([False] * len(merged), index=merged.index)
    for col in compare_cols:
        src_col = f"{col}_src"
        tgt_col = f"{col}_tgt"
        if src_col not in merged or tgt_col not in merged:
            continue
        if pd.api.types.is_numeric_dtype(merged[src_col]):
            diff = (merged[src_col] - merged[tgt_col]).abs()
            mismatch_mask |= diff > tolerance
        else:
            mismatch_mask |= merged[src_col].astype(str) != merged[tgt_col].astype(str)

    return merged[mismatch_mask]


def chunk_dataframe(df: pd.DataFrame, chunk_size: int = 10000):
    """Generator to process large DataFrames in chunks."""
    for i in range(0, len(df), chunk_size):
        yield df.iloc[i: i + chunk_size]


def generate_test_summary_table(results: list) -> pd.DataFrame:
    """Convert a list of validator result dicts into a summary DataFrame."""
    return pd.DataFrame([
        {
            "check": r.get("check"),
            "status": "PASS" if r.get("passed") else "FAIL",
            "details": r.get("details", "")[:200],
        }
        for r in results
    ])


def assert_no_failures(results: list, suite_name: str = "ETL Check Suite"):
    """
    Bulk assert — raise if any results in the list failed.
    Prints a table of all failures.
    """
    failures = [r for r in results if not r.get("passed")]
    if failures:
        summary = "\n".join(
            f"  ❌ {r['check']}: {r['details']}" for r in failures
        )
        raise AssertionError(
            f"\n{suite_name} — {len(failures)} failure(s):\n{summary}"
        )
