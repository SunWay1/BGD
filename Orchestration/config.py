"""
Central config — paths, MongoDB settings, and filter thresholds.

Override MongoDB defaults with env vars:
    MONGO_URI       mongodb://localhost:27017
    MONGO_DB_NAME   bgd_taxidb
"""

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
RAW_DATA_DIR = Path(os.getenv("RAW_DATA_DIR", str(
    Path(__file__).parent / "data" / "raw")))
DUCKDB_FILE = Path(os.getenv("DUCKDB_PATH", str(
    Path(__file__).parent / "taxi_etl.duckdb")))
DBT_PROJECT_DIR = Path(__file__).parent / "dbt_project"

MONGO_URI = os.getenv("MONGO_URI",     "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "bgd_taxidb")
REDIS_URL = os.getenv("REDIS_URL",     "redis://localhost:6379")

EXPECTED_PARQUET_FILE_COUNT = 24  # 12 months × 2 years
ZONES_CSV_FILENAME = "taxi_zone_lookup.csv"


FILTER_FARE_MIN = 0
FILTER_FARE_MAX = 500
FILTER_DISTANCE_MIN = 0
FILTER_DISTANCE_MAX = 200
FILTER_PASSENGER_MIN = 1
FILTER_PASSENGER_MAX = 8
FILTER_DURATION_MIN_MINUTES = 1
FILTER_DURATION_MAX_MINUTES = 300

GOLD_TOP_ROUTES_MIN_TRIPS = 1_000
GOLD_TOP_ROUTES_LIMIT = 50

MIN_CLEAN_TRIP_RETENTION_RATE = 0.75
EXPECTED_HOURLY_DEMAND_ROWS = 24

AIRPORT_ZONE_NAMES = frozenset({
    "JFK Airport",
    "LaGuardia Airport",
    "Newark Airport",
})

MONGO_INSERT_BATCH_SIZE = 50_000
MONGO_UPSERT_BATCH_SIZE = 500
