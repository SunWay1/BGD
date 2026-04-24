#!/usr/bin/env python3
"""
NYC Taxi ETL Pipeline — Prefect + Redis Streams + DuckDB + dbt

Run from the repo root:
    python Orchestration/pipeline.py              # idempotent, skips already-built layers
    python Orchestration/pipeline.py --force      # full rebuild
    python Orchestration/pipeline.py --duckdb-only  # transform only, no MongoDB writes

Prerequisites:
    pip install -r Orchestration/requirements.txt
    MongoDB and Redis running (see docker-compose.yml)
    24 parquet files in Orchestration/data/raw/  (run Orchestration/download_data.sh if missing)

See Orchestration/architecture.md for the full pipeline diagram.
"""

from mongodb_loader import (
    get_database,
    ensure_all_indexes,
    upsert_clean_zones,
    load_clean_trips,
    upsert_gold_zone_revenue,
    upsert_gold_hourly_demand,
    upsert_gold_top_routes,
    record_pipeline_run,
    sanitize_records,
)
from duckdb_processor import TaxiDataProcessor
from data_quality import assert_raw_layer_loaded, assert_clean_trips_retention
import config
import load_raw_data
import redis_queue
from prefect import flow, task, get_run_logger
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


logging.basicConfig(
    format="%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger("nyc_taxi_pipeline")


@task(name="validate-environment", retries=1, retry_delay_seconds=10)
def validate_environment() -> None:
    """Checks parquet files, zones CSV, MongoDB, Redis, and dbt installation."""
    task_logger = get_run_logger()

    parquet_files = sorted(config.RAW_DATA_DIR.glob("yellow_tripdata_*.parquet"))
    if len(parquet_files) < config.EXPECTED_PARQUET_FILE_COUNT:
        raise FileNotFoundError(
            f"Expected {config.EXPECTED_PARQUET_FILE_COUNT} parquet files in "
            f"{config.RAW_DATA_DIR}, found {len(parquet_files)}.\n"
            f"Run: bash Orchestration/download_data.sh"
        )

    zones_csv = config.RAW_DATA_DIR / config.ZONES_CSV_FILENAME
    if not zones_csv.exists():
        raise FileNotFoundError(
            f"{config.ZONES_CSV_FILENAME} not found at {zones_csv}.\n"
            f"Run: bash Orchestration/download_data.sh"
        )

    get_database()  # raises if MongoDB is unreachable

    client = redis_queue.get_client()
    client.ping()   # raises if Redis is unreachable
    client.close()

    result = subprocess.run(["dbt", "--version"], capture_output=True)
    if result.returncode != 0:
        raise RuntimeError("dbt not found — run: pip install dbt-duckdb")

    task_logger.info(
        f"Environment OK — {len(parquet_files)} parquet files, "
        f"MongoDB at {config.MONGO_URI}, Redis at {config.REDIS_URL}"
    )


@task(name="load-raw-via-queue", retries=1, retry_delay_seconds=30)
def load_raw_via_queue(force: bool = False) -> dict:
    """
    Loads raw data through Redis Streams, one parquet file at a time.
    Each file is published then immediately consumed so Redis never holds
    more than ~one file worth of records in memory.
    """
    task_logger = get_run_logger()
    db = get_database()

    existing_trips = db["raw_trips"].count_documents({})
    existing_zones = db["raw_zones"].count_documents({})

    if not force and existing_trips > 0 and existing_zones > 0:
        task_logger.info(
            f"Raw data already in MongoDB (trips: {existing_trips:,}, zones: {existing_zones}) — skipping."
        )
        assert_raw_layer_loaded(existing_trips, existing_zones)
        return {"raw_trips": existing_trips, "raw_zones": existing_zones}

    client = redis_queue.get_client()

    # zones: small, publish all then consume
    redis_queue.flush(client, redis_queue.STREAM_RAW_ZONES)
    load_raw_data.publish_zones(client)

    db["raw_zones"].drop()
    zones_records = list(redis_queue.consume(client, redis_queue.STREAM_RAW_ZONES))
    for row in zones_records:
        row["LocationID"] = int(row["LocationID"])
    db["raw_zones"].insert_many(zones_records)
    task_logger.info(f"raw_zones: {len(zones_records)} loaded")

    if force:
        db["raw_trips"].drop()

    # trips: one file at a time — keeps Redis memory bounded to ~one parquet file
    parquet_files = sorted(load_raw_data.DATA_DIR.glob("yellow_tripdata_*.parquet"))
    total_trips = 0

    for filepath in parquet_files:
        task_logger.info(f"Processing {filepath.name}...")
        redis_queue.flush(client, redis_queue.STREAM_RAW_TRIPS)

        published = load_raw_data.publish_file(client, filepath)

        batch = []
        for record in redis_queue.consume(client, redis_queue.STREAM_RAW_TRIPS):
            batch.append(record)
            if len(batch) >= 10_000:
                db["raw_trips"].insert_many(batch, ordered=False)
                total_trips += len(batch)
                batch = []
        if batch:
            db["raw_trips"].insert_many(batch, ordered=False)
            total_trips += len(batch)

        task_logger.info(f"  {filepath.name}: {published:,} records loaded")

    task_logger.info(f"raw_trips total: {total_trips:,}")
    assert_raw_layer_loaded(total_trips, len(zones_records))
    return {"raw_trips": total_trips, "raw_zones": len(zones_records)}


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
    """Runs `dbt test` to validate all schema constraints (not_null, unique)."""
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
        raw_count = proc.get_raw_trip_count()
        clean_count = proc._conn.execute("SELECT count(*) FROM clean_trips").fetchone()[0]

    assert_clean_trips_retention(raw_count, clean_count)


@task(name="produce-processed-to-queue")
def produce_processed_to_queue(force: bool = False, skip: bool = False) -> dict:
    """
    Reads all dbt output tables from DuckDB and publishes them to Redis Streams.
    Skips if clean_trips is already in MongoDB and force=False.
    """
    task_logger = get_run_logger()

    if skip:
        task_logger.info("--duckdb-only set — skipping processed queue")
        return {"skipped": True}

    db = get_database()
    if not force and db["clean_trips"].count_documents({}) > 0:
        task_logger.info("Processed data already in MongoDB — skipping. Use --force to reload.")
        return {"skipped": True}

    client = redis_queue.get_client()
    for stream in [
        redis_queue.STREAM_CLEAN_ZONES, redis_queue.STREAM_CLEAN_TRIPS,
        redis_queue.STREAM_GOLD_REVENUE, redis_queue.STREAM_GOLD_HOURLY,
        redis_queue.STREAM_GOLD_ROUTES,
    ]:
        redis_queue.flush(client, stream)

    with TaxiDataProcessor() as proc:
        zones = proc.fetch_all_records("clean_zones")
        redis_queue.publish(client, redis_queue.STREAM_CLEAN_ZONES, zones)
        task_logger.info(f"Published {len(zones)} clean_zones")

        # clean_trips is large (~70M rows) — publish and consume chunk by chunk
        # so Redis never accumulates more than one DuckDB batch at a time
        trips_count = 0
        db_temp = get_database()
        if db_temp["clean_trips"].count_documents({}) > 0:
            db_temp["clean_trips"].delete_many({})

        for batch in proc.stream_records_in_batches("clean_trips"):
            clean_batch = sanitize_records(batch)
            redis_queue.flush(client, redis_queue.STREAM_CLEAN_TRIPS)
            redis_queue.publish(client, redis_queue.STREAM_CLEAN_TRIPS, clean_batch)
            mongo_batch = list(redis_queue.consume(client, redis_queue.STREAM_CLEAN_TRIPS))
            db_temp["clean_trips"].insert_many(mongo_batch, ordered=False)
            trips_count += len(mongo_batch)
            if trips_count % 500_000 == 0:
                task_logger.info(f"  clean_trips: {trips_count:,} loaded so far")
        task_logger.info(f"clean_trips: {trips_count:,} loaded")

        gold_rev = proc.fetch_all_records("gold_zone_revenue")
        redis_queue.publish(client, redis_queue.STREAM_GOLD_REVENUE, sanitize_records(gold_rev))

        hourly = proc.fetch_all_records("gold_hourly_demand")
        redis_queue.publish(client, redis_queue.STREAM_GOLD_HOURLY, sanitize_records(hourly))

        routes = proc.fetch_all_records("gold_top_routes")
        redis_queue.publish(client, redis_queue.STREAM_GOLD_ROUTES, sanitize_records(routes))

    return {
        "skipped":          False,
        "clean_zones":      len(zones),
        "clean_trips":      trips_count,
        "gold_zone_revenue": len(gold_rev),
        "gold_hourly_demand": len(hourly),
        "gold_top_routes":  len(routes),
    }


@task(name="consume-processed-from-queue", retries=2, retry_delay_seconds=30)
def consume_processed_from_queue(produce_result: dict) -> dict:
    """Drains all processed streams and writes them to MongoDB clean and gold layers."""
    task_logger = get_run_logger()

    if produce_result.get("skipped"):
        task_logger.info("Processed produce was skipped — nothing to consume.")
        return {"skipped": True}

    db = get_database()
    client = redis_queue.get_client()
    stats = {}

    task_logger.info("Loading clean_zones...")
    zones_records = list(redis_queue.consume(client, redis_queue.STREAM_CLEAN_ZONES))
    upsert_clean_zones(db, zones_records)
    stats["clean_zones"] = len(zones_records)

    # clean_trips is handled inside produce_processed_to_queue (chunk-by-chunk)
    stats["clean_trips"] = db["clean_trips"].count_documents({})

    task_logger.info("Loading gold layer...")
    gold_rev = list(redis_queue.consume(client, redis_queue.STREAM_GOLD_REVENUE))
    upsert_gold_zone_revenue(db, gold_rev)
    stats["gold_zone_revenue"] = len(gold_rev)

    hourly = list(redis_queue.consume(client, redis_queue.STREAM_GOLD_HOURLY))
    upsert_gold_hourly_demand(db, hourly)
    stats["gold_hourly_demand"] = len(hourly)

    routes = list(redis_queue.consume(client, redis_queue.STREAM_GOLD_ROUTES))
    upsert_gold_top_routes(db, routes)
    stats["gold_top_routes"] = len(routes)

    task_logger.info(f"MongoDB load complete — {stats}")
    return stats


@flow(
    name="nyc-taxi-etl-pipeline",
    description=(
        "NYC Yellow Taxi 2022-2023 medallion pipeline. "
        "24 parquet files → Redis Streams → Raw → dbt/DuckDB → Redis Streams → Clean/Gold in MongoDB."
    ),
)
def nyc_taxi_etl_pipeline(force: bool = False, duckdb_only: bool = False) -> dict:
    """
    Runs all pipeline stages end-to-end.

    force=True    — rebuilds all dbt models and reloads all MongoDB collections
    duckdb_only   — stops after dbt transforms, skips MongoDB writes for clean/gold
    """
    flow_logger = get_run_logger()
    t0 = time.time()
    flow_logger.info(f"pipeline starting | force={force} | duckdb_only={duckdb_only}")

    validate_environment()

    raw_stats = load_raw_via_queue(force=force)

    ensure_mongodb_indexes()
    source_stats = create_duckdb_source_views()
    run_dbt_transformations(force=force)
    run_dbt_tests()
    check_retention_rate()

    processed_produce = produce_processed_to_queue(force=force, skip=duckdb_only)
    load_stats        = consume_processed_from_queue(processed_produce)

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
        description="NYC Taxi ETL Pipeline — Prefect + Redis Streams + DuckDB + dbt",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python Orchestration/pipeline.py                # normal run\n"
            "  python Orchestration/pipeline.py --force        # full rebuild\n"
            "  python Orchestration/pipeline.py --duckdb-only  # no MongoDB writes\n"
        ),
    )
    parser.add_argument("--force",       action="store_true",
                        help="Rebuild all dbt models and reload all collections")
    parser.add_argument("--duckdb-only", action="store_true", dest="duckdb_only",
                        help="Skip MongoDB writes for clean and gold layers")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    result = nyc_taxi_etl_pipeline(force=args.force, duckdb_only=args.duckdb_only)

    print("\n" + "=" * 60)
    print("Pipeline summary")
    print("=" * 60)
    print(json.dumps(result, indent=2, default=str))
