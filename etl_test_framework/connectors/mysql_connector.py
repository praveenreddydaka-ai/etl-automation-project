"""
connectors/mysql_connector.py
------------------------------
Robust MySQL connector using SQLAlchemy with connection pooling,
retry logic, and a clean query interface returning Pandas DataFrames.
"""

import time
import logging
from contextlib import contextmanager
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import OperationalError

logger = logging.getLogger(__name__)


class MySQLConnector:
    """
    Enterprise MySQL connector supporting:
    - Connection pooling
    - Automatic retry on transient failures
    - Query execution returning DataFrames
    - Schema introspection
    """

    def __init__(self, db_config: dict, label: str = "db", retries: int = 3):
        self.label = label
        self.retries = retries
        self.engine = self._create_engine(db_config)
        logger.info(f"[{self.label}] MySQL connector initialized → {db_config['database']}")

    def _create_engine(self, cfg: dict):
        url = (
            f"mysql+pymysql://{cfg['username']}:{cfg['password']}"
            f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
            f"?charset=utf8mb4"
        )
        return create_engine(
            url,
            poolclass=QueuePool,
            pool_size=cfg.get("pool_size", 5),
            max_overflow=cfg.get("max_overflow", 10),
            pool_pre_ping=True,
            pool_recycle=3600,
        )

    @contextmanager
    def _get_connection(self):
        """Context manager for connection with retry logic."""
        attempt = 0
        while attempt < self.retries:
            try:
                with self.engine.connect() as conn:
                    yield conn
                    return
            except OperationalError as e:
                attempt += 1
                wait = 2 ** attempt
                logger.warning(
                    f"[{self.label}] Connection failed (attempt {attempt}/{self.retries}). "
                    f"Retrying in {wait}s... Error: {e}"
                )
                time.sleep(wait)
        raise ConnectionError(
            f"[{self.label}] Failed to connect after {self.retries} attempts."
        )

    def query(self, sql: str, params: Optional[dict] = None) -> pd.DataFrame:
        """Execute SQL and return results as a DataFrame."""
        logger.debug(f"[{self.label}] Executing: {sql[:120]}...")
        with self._get_connection() as conn:
            return pd.read_sql(text(sql), conn, params=params)

    def get_table_df(self, table_name: str, where: str = None, limit: int = None) -> pd.DataFrame:
        """Fetch a full table or subset as a DataFrame."""
        sql = f"SELECT * FROM `{table_name}`"
        if where:
            sql += f" WHERE {where}"
        if limit:
            sql += f" LIMIT {limit}"
        return self.query(sql)

    def get_row_count(self, table_name: str, where: str = None) -> int:
        """Get row count for a table."""
        sql = f"SELECT COUNT(*) as cnt FROM `{table_name}`"
        if where:
            sql += f" WHERE {where}"
        result = self.query(sql)
        return int(result["cnt"].iloc[0])

    def get_table_schema(self, table_name: str) -> pd.DataFrame:
        """Fetch column schema from information_schema."""
        sql = """
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                COLUMN_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                COLUMN_KEY,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = :table_name
            ORDER BY ORDINAL_POSITION
        """
        return self.query(sql, params={"table_name": table_name})

    def get_all_tables(self) -> list:
        """List all tables in the current database."""
        inspector = inspect(self.engine)
        return inspector.get_table_names()

    def table_exists(self, table_name: str) -> bool:
        return table_name in self.get_all_tables()

    def dispose(self):
        """Clean up connection pool."""
        self.engine.dispose()
        logger.info(f"[{self.label}] Connection pool disposed.")
