"""
DuckDB helper for the NYC Taxi ETL pipeline.

Responsibilities after introducing dbt:
  - create_source_views()      — creates raw_trips / raw_zones in DuckDB before dbt runs
  - fetch_all_records()        — exports small tables to Python dicts (for MongoDB upserts)
  - stream_records_in_batches() — streams large tables in batches (for clean_trips bulk insert)

All transformation logic (clean / gold layers) now lives in dbt_project/models/.
"""

import logging
from pathlib import Path
from typing import Generator

import duckdb
import pandas as pd

from config import RAW_DATA_DIR, DUCKDB_FILE, MONGO_INSERT_BATCH_SIZE

logger = logging.getLogger(__name__)


class TaxiDataProcessor:
    """Thin DuckDB wrapper — source view setup and data export for MongoDB loading."""

    def __init__(self, db_path: Path = DUCKDB_FILE):
        self._db_path = db_path
        self._conn: duckdb.DuckDBPyConnection | None = None

    def __enter__(self) -> "TaxiDataProcessor":
        self._conn = duckdb.connect(str(self._db_path))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def _parquet_glob(self) -> str:
        return str(RAW_DATA_DIR / "yellow_tripdata_*.parquet")

    def _zones_csv(self) -> str:
        return str(RAW_DATA_DIR / "taxi_zone_lookup.csv")

    def _table_exists(self, table_name: str) -> bool:
        result = self._conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
        ).fetchone()
        return result[0] > 0

    def create_source_views(self) -> None:
        """
        Creates raw_trips and raw_zones in DuckDB so dbt models can reference them
        via {{ source('raw', 'raw_trips') }} and {{ source('raw', 'raw_zones') }}.

        raw_trips — a VIEW over the parquet files (zero copy, DuckDB scans on demand).
        raw_zones — a small TABLE materialized from CSV (265 rows, negligible cost).
        """
        self._conn.execute(f"""
            CREATE OR REPLACE VIEW raw_trips AS
            SELECT * FROM read_parquet('{self._parquet_glob()}', union_by_name = true)
        """)
        self._conn.execute(f"""
            CREATE OR REPLACE TABLE raw_zones AS
            SELECT
                CAST(LocationID AS INTEGER) AS LocationID,
                Borough,
                Zone,
                service_zone
            FROM read_csv_auto('{self._zones_csv()}', header = true)
        """)
        logger.info("DuckDB source views created: raw_trips (view), raw_zones (table)")

    def get_raw_trip_count(self) -> int:
        count = self._conn.execute(
            f"SELECT count(*) FROM read_parquet('{self._parquet_glob()}', union_by_name=true)"
        ).fetchone()[0]
        logger.info(f"Raw parquet scan: {count:,} trips")
        return count

    def all_dbt_models_exist(self) -> bool:
        """Returns True if all five dbt output tables are already present in DuckDB."""
        tables = ["clean_zones", "clean_trips", "gold_zone_revenue", "gold_hourly_demand", "gold_top_routes"]
        return all(self._table_exists(t) for t in tables)

    def fetch_all_records(self, table_name: str) -> list[dict]:
        """Returns all rows as a list of dicts. Only call this on small tables."""
        df: pd.DataFrame = self._conn.execute(f"SELECT * FROM {table_name}").fetchdf()
        return df.to_dict("records")

    def stream_records_in_batches(
        self,
        table_name: str,
        batch_size: int = MONGO_INSERT_BATCH_SIZE,
    ) -> Generator[list[dict], None, None]:
        """Yields rows in batches of dicts. Use for large tables (clean_trips ~11M rows)."""
        offset = 0
        while True:
            df: pd.DataFrame = self._conn.execute(
                f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
            ).fetchdf()

            if df.empty:
                break

            yield df.to_dict("records")
            offset += batch_size

            if len(df) < batch_size:
                break
