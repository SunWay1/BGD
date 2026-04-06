#!/usr/bin/env python3
"""
NYC Taxi ETL Pipeline — Prefect + DuckDB + dbt

Run from the repo root:
    python Orchestration/pipeline.py              # idempotent, skips already-built layers
    python Orchestration/pipeline.py --force      # full rebuild
    python Orchestration/pipeline.py --duckdb-only  # transform only, no MongoDB writes

Prerequisites:
    pip install -r Orchestration/requirements.txt
    MongoDB running (default localhost:27017, override with MONGO_URI env var)
    24 parquet files in ETL_task1/data/raw/  (run ETL_task1/scripts/download_data.sh if missing)

See Orchestration/architecture.md for the full pipeline diagram.
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from prefect import flow, task, get_run_logger

import config
from data_quality import assert_raw_layer_loaded, assert_clean_trips_retention
from duckdb_processor import TaxiDataProcessor
from mongodb_loader import (
    get_database,
    ensure_all_indexes,
    upsert_clean_zones,
    load_clean_trips,
    upsert_gold_zone_revenue,
    upsert_gold_hourly_demand,
    upsert_gold_top_routes,
    record_pipeline_run,
)

logging.basicConfig(
    format="%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger("nyc_taxi_pipeline")


@task(name="validate-environment", retries=1, retry_delay_seconds=10)
def validate_environment() -> None:
    """Checks parquet files, zones CSV, MongoDB, and dbt installation."""
    task_logger = get_run_logger()

    parquet_files = sorted(config.RAW_DATA_DIR.glob("yellow_tripdata_*.parquet"))
    if len(parquet_files) < config.EXPECTED_PARQUET_FILE_COUNT:
        raise FileNotFoundError(
            f"Expected {config.EXPECTED_PARQUET_FILE_COUNT} parquet files in "
            f"{config.RAW_DATA_DIR}, found {len(parquet_files)}.\n"
            f"Run: bash ETL_task1/scripts/download_data.sh"
        )

    zones_csv = config.RAW_DATA_DIR / config.ZONES_CSV_FILENAME
    if not zones_csv.exists():
        raise FileNotFoundError(
            f"{config.ZONES_CSV_FILENAME} not found at {zones_csv}.\n"
            f"Run: bash ETL_task1/scripts/download_data.sh"
        )

    get_database()  # raises if MongoDB is unreachable

    result = subprocess.run(["dbt", "--version"], capture_output=True)
    if result.returncode != 0:
        raise RuntimeError("dbt not found — run: pip install dbt-duckdb")

    task_logger.info(
        f"Environment OK — {len(parquet_files)} parquet files, MongoDB at {config.MONGO_URI}"
    )


@task(name="load-raw-to-mongodb", retries=1, retry_delay_seconds=30)
def load_raw_to_mongodb(force: bool = False) -> dict:
    """
    Loads parquet files → raw_trips and zones CSV → raw_zones via load_data.py.
    Skips if both collections are already populated.
    """
    task_logger = get_run_logger()
    db = get_database()

    existing_trips = db["raw_trips"].count_documents({})
    existing_zones = db["raw_zones"].count_documents({})

    if not force and existing_trips > 0 and existing_zones > 0:
        task_logger.info(
            f"Raw data already loaded — trips: {existing_trips:,}, zones: {existing_zones}. Skipping."
        )
        assert_raw_layer_loaded(existing_trips, existing_zones)
        return {"raw_trips": existing_trips, "raw_zones": existing_zones, "loaded": False}

    load_script = Path(__file__).parent / "load_raw_data.py"
    task_logger.info(f"Loading raw data from {config.RAW_DATA_DIR} ...")

    result = subprocess.run(
        [sys.executable, str(load_script)],
        capture_output=True, text=True,
        cwd=str(config.PROJECT_ROOT),
    )
    if result.returncode != 0:
        raise RuntimeError(f"load_data.py failed:\nstderr:\n{result.stderr}")

    new_trips = db["raw_trips"].count_documents({})
    new_zones = db["raw_zones"].count_documents({})

    assert_raw_layer_loaded(new_trips, new_zones)
    task_logger.info(f"Raw data loaded — trips: {new_trips:,}, zones: {new_zones}")
    return {"raw_trips": new_trips, "raw_zones": new_zones, "loaded": True}


@task(name="ensure-mongodb-indexes")
def ensure_mongodb_indexes() -> None:
    ensure_all_indexes(get_database())
    get_run_logger().info("Indexes verified")


@task(name="create-duckdb-source-views")
def create_duckdb_source_views() -> dict:
    """
    Creates raw_trips (view) and raw_zones (table) in DuckDB so dbt models
    can reference them via {{ source('raw', 'raw_trips') }}.
    Must run before dbt.
    """
    task_logger = get_run_logger()
    with TaxiDataProcessor() as proc:
        proc.create_source_views()
        raw_count = proc.get_raw_trip_count()

    task_logger.info(f"Source views ready — {raw_count:,} raw trips available to dbt")
    return {"raw_trips": raw_count}


@task(name="run-dbt-transformations", retries=1, retry_delay_seconds=60)
def run_dbt_transformations(force: bool = False) -> None:
    """
    Runs `dbt run` to build clean and gold layers.

    Skips if all five output tables already exist and force=False.
    force=True maps to `dbt run --full-refresh`.
    """
    task_logger = get_run_logger()

    if not force:
        with TaxiDataProcessor() as proc:
            if proc.all_dbt_models_exist():
                task_logger.info("All dbt models already exist — skipping. Use --force to rebuild.")
                return

    env = {**os.environ, "DUCKDB_PATH": str(config.DUCKDB_FILE)}
    cmd = [
        "dbt", "run",
        "--project-dir", str(config.DBT_PROJECT_DIR),
        "--profiles-dir", str(config.DBT_PROJECT_DIR),
    ]
    if force:
        cmd.append("--full-refresh")

    task_logger.info(f"Running dbt: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    task_logger.info(result.stdout)

    if result.returncode != 0:
        raise RuntimeError(f"dbt run failed:\n{result.stdout}\n{result.stderr}")


@task(name="run-dbt-tests")
def run_dbt_tests() -> None:
    """
    Runs `dbt test` to validate all schema constraints (not_null, unique)
    defined in models/clean/schema.yml and models/gold/schema.yml.
    """
    task_logger = get_run_logger()
    env = {**os.environ, "DUCKDB_PATH": str(config.DUCKDB_FILE)}

    result = subprocess.run(
        [
            "dbt", "test",
            "--project-dir", str(config.DBT_PROJECT_DIR),
            "--profiles-dir", str(config.DBT_PROJECT_DIR),
        ],
        capture_output=True, text=True, env=env,
    )
    task_logger.info(result.stdout)

    if result.returncode != 0:
        raise RuntimeError(f"dbt test failed:\n{result.stdout}\n{result.stderr}")


@task(name="check-retention-rate")
def check_retention_rate() -> None:
    """Checks clean_trips retention vs raw (ratio test — not covered by dbt tests)."""
    with TaxiDataProcessor() as proc:
        raw_count   = proc.get_raw_trip_count()
        clean_count = proc._conn.execute("SELECT count(*) FROM clean_trips").fetchone()[0]

    assert_clean_trips_retention(raw_count, clean_count)


@task(name="load-processed-to-mongodb", retries=2, retry_delay_seconds=30)
def load_processed_to_mongodb(force: bool = False, skip: bool = False) -> dict:
    """
    Writes dbt output tables to MongoDB using upserts and guarded inserts.
    All writes are safe to re-run.
    """
    if skip:
        get_run_logger().info("--duckdb-only set — skipping MongoDB write")
        return {"skipped": True}

    task_logger = get_run_logger()
    db = get_database()
    stats = {}

    with TaxiDataProcessor() as proc:
        task_logger.info("Loading clean_zones ...")
        zones_records = proc.fetch_all_records("clean_zones")
        upsert_clean_zones(db, zones_records)
        stats["clean_zones"] = len(zones_records)

        task_logger.info("Loading clean_trips (may take a few minutes) ...")
        inserted = load_clean_trips(db, proc.stream_records_in_batches("clean_trips"), force=force)
        stats["clean_trips"] = inserted if inserted > 0 else db["clean_trips"].count_documents({})

        task_logger.info("Loading gold layer ...")
        zone_rev = proc.fetch_all_records("gold_zone_revenue")
        upsert_gold_zone_revenue(db, zone_rev)
        stats["gold_zone_revenue"] = len(zone_rev)

        hourly = proc.fetch_all_records("gold_hourly_demand")
        upsert_gold_hourly_demand(db, hourly)
        stats["gold_hourly_demand"] = len(hourly)

        routes = proc.fetch_all_records("gold_top_routes")
        upsert_gold_top_routes(db, routes)
        stats["gold_top_routes"] = len(routes)

    task_logger.info(f"MongoDB load complete — {stats}")
    return stats


@flow(
    name="nyc-taxi-etl-pipeline",
    description=(
        "NYC Yellow Taxi 2022-2023 medallion pipeline. "
        "24 parquet files → Raw → Clean → Gold via dbt + DuckDB, stored in MongoDB."
    ),
)
def nyc_taxi_etl_pipeline(force: bool = False, duckdb_only: bool = False) -> dict:
    """
    Runs all pipeline stages end-to-end.

    force=True      — rebuilds all dbt models and reloads clean_trips into MongoDB
    duckdb_only     — stops after dbt transforms, skips MongoDB writes
    """
    flow_logger = get_run_logger()
    t0 = time.time()
    flow_logger.info(f"pipeline starting | force={force} | duckdb_only={duckdb_only}")

    validate_environment()
    raw_stats    = load_raw_to_mongodb(force=force)
    ensure_mongodb_indexes()
    source_stats = create_duckdb_source_views()
    run_dbt_transformations(force=force)
    run_dbt_tests()
    check_retention_rate()
    load_stats   = load_processed_to_mongodb(force=force, skip=duckdb_only)

    elapsed = round(time.time() - t0, 1)
    summary = {
        "status":          "SUCCESS",
        "elapsed_seconds": elapsed,
        "force":           force,
        "duckdb_only":     duckdb_only,
        "raw":             raw_stats,
        "source":          source_stats,
        "mongodb_load":    load_stats,
    }

    if not duckdb_only:
        record_pipeline_run(get_database(), {
            "pipeline":        "nyc-taxi-etl",
            "status":          "SUCCESS",
            "started_at":      datetime.now(timezone.utc),
            "elapsed_seconds": elapsed,
            "force":           force,
            "stats":           summary,
        })

    flow_logger.info(f"pipeline done in {elapsed}s")
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python Orchestration/pipeline.py",
        description="NYC Taxi ETL Pipeline — Prefect + DuckDB + dbt",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python Orchestration/pipeline.py                # normal run\n"
            "  python Orchestration/pipeline.py --force        # full rebuild\n"
            "  python Orchestration/pipeline.py --duckdb-only  # no MongoDB writes\n"
        ),
    )
    parser.add_argument("--force",       action="store_true",
                        help="Rebuild all dbt models and reload clean_trips")
    parser.add_argument("--duckdb-only", action="store_true", dest="duckdb_only",
                        help="Skip MongoDB writes")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    result = nyc_taxi_etl_pipeline(force=args.force, duckdb_only=args.duckdb_only)

    print("\n" + "=" * 60)
    print("Pipeline summary")
    print("=" * 60)
    print(json.dumps(result, indent=2, default=str))
