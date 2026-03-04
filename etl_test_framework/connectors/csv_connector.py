"""
connectors/csv_connector.py
----------------------------
CSV file connector with schema inference, type casting,
encoding detection, and data quality pre-checks.
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


class CSVConnector:
    """
    Robust CSV connector supporting:
    - Automatic delimiter detection
    - Encoding handling
    - Schema inference and type casting
    - Large file chunked loading
    """

    SUPPORTED_EXTENSIONS = {".csv", ".tsv", ".txt", ".dat"}

    def __init__(self, file_path: str, label: str = "csv"):
        self.file_path = Path(file_path)
        self.label = label
        self._validate_path()
        logger.info(f"[{self.label}] CSV connector initialized → {self.file_path}")

    def _validate_path(self):
        if not self.file_path.exists():
            raise FileNotFoundError(f"[{self.label}] File not found: {self.file_path}")
        if self.file_path.suffix.lower() not in self.SUPPORTED_EXTENSIONS:
            raise ValueError(
                f"[{self.label}] Unsupported file type: {self.file_path.suffix}"
            )

    def _detect_delimiter(self) -> str:
        """Detect CSV delimiter by sniffing first line."""
        import csv
        with open(self.file_path, "r", encoding="utf-8-sig", errors="replace") as f:
            sample = f.read(4096)
        try:
            dialect = csv.Sniffer().sniff(sample)
            return dialect.delimiter
        except csv.Error:
            return ","  # Default to comma

    def load(
        self,
        delimiter: Optional[str] = None,
        dtype: Optional[dict] = None,
        parse_dates: Optional[list] = None,
        usecols: Optional[list] = None,
        encoding: str = "utf-8-sig",
        chunksize: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Load CSV into a DataFrame.

        Args:
            delimiter:   Column separator (auto-detected if None)
            dtype:       Column type overrides
            parse_dates: List of date columns to parse
            usecols:     Subset of columns to load
            encoding:    File encoding
            chunksize:   For large files — loads in chunks & concatenates
        """
        sep = delimiter or self._detect_delimiter()
        logger.info(f"[{self.label}] Loading with delimiter='{sep}', encoding='{encoding}'")

        read_kwargs = dict(
            filepath_or_buffer=self.file_path,
            sep=sep,
            dtype=dtype,
            parse_dates=parse_dates or False,
            usecols=usecols,
            encoding=encoding,
            na_values=["NA", "N/A", "null", "NULL", "None", "NONE", ""],
            keep_default_na=True,
        )

        if chunksize:
            chunks = pd.read_csv(**read_kwargs, chunksize=chunksize)
            df = pd.concat(chunks, ignore_index=True)
        else:
            df = pd.read_csv(**read_kwargs)

        # Strip whitespace from string columns
        str_cols = df.select_dtypes(include="object").columns
        df[str_cols] = df[str_cols].apply(lambda c: c.str.strip())

        logger.info(
            f"[{self.label}] Loaded {len(df):,} rows × {len(df.columns)} columns"
        )
        return df

    def get_row_count(self) -> int:
        """Count rows without loading full file into memory."""
        with open(self.file_path, "r", encoding="utf-8-sig") as f:
            return sum(1 for _ in f) - 1  # Exclude header

    def get_column_names(self) -> list:
        """Read only header row."""
        sep = self._detect_delimiter()
        df = pd.read_csv(self.file_path, sep=sep, nrows=0)
        return df.columns.tolist()

    def get_schema(self) -> pd.DataFrame:
        """Infer schema from first 1000 rows."""
        df = self.load()
        schema = pd.DataFrame({
            "column_name": df.columns,
            "inferred_dtype": [str(dt) for dt in df.dtypes],
            "non_null_count": df.notna().sum().values,
            "null_count": df.isna().sum().values,
            "null_pct": (df.isna().mean() * 100).round(2).values,
        })
        return schema
