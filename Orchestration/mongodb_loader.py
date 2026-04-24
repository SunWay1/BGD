"""
MongoDB writer for the ETL pipeline.

We avoid dropping collections because that destroys indexes, which are slow
to rebuild on 10M+ row collections. Instead:
  - Small collections with natural keys (zones, gold): upsert
  - clean_trips: skip if already populated; delete_many + bulk insert otherwise
    (delete_many keeps the collection shell + indexes intact)

Collections written here:
  clean_zones         upsert by LocationID
  clean_trips         skip-or-reload
  gold_zone_revenue   upsert by location_id
  gold_hourly_demand  upsert by pickup_hour
  gold_top_routes     upsert by (pu_location_id, do_location_id)
  pipeline_runs       append-only
"""

import logging
from datetime import datetime, timezone
from typing import Generator

import numpy as np
import pandas as pd
import pymongo
from pymongo import MongoClient, ReplaceOne
from pymongo.database import Database

from config import (
    MONGO_URI,
    MONGO_DB_NAME,
    MONGO_INSERT_BATCH_SIZE,
    MONGO_UPSERT_BATCH_SIZE,
)

logger = logging.getLogger(__name__)


def get_database() -> Database:
    """Returns the target database. Raises if MongoDB is unreachable."""
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5_000)
    client.admin.command("ping")
    return client[MONGO_DB_NAME]


def ensure_all_indexes(db: Database) -> None:
    """
    Creates indexes for all collections. create_index is a no-op if the index
    already exists, so this is safe to call on every run.
    """
    # raw_trips — kept in sync with 01_raw_layer.js / 02_clean_layer.js expectations
    db["raw_trips"].create_index([("PULocationID",         pymongo.ASCENDING)])
    db["raw_trips"].create_index([("DOLocationID",         pymongo.ASCENDING)])
    db["raw_trips"].create_index([("tpep_pickup_datetime", pymongo.ASCENDING)])
    db["raw_trips"].create_index([("__sourceFile",         pymongo.ASCENDING)])
    db["raw_trips"].create_index([("fare_amount",          pymongo.ASCENDING)])
    db["raw_trips"].create_index([("trip_distance",        pymongo.ASCENDING)])
    db["raw_trips"].create_index([("passenger_count",      pymongo.ASCENDING)])

    db["clean_trips"].create_index([("PULocationID",  pymongo.ASCENDING)])
    db["clean_trips"].create_index([("DOLocationID",  pymongo.ASCENDING)])
    db["clean_trips"].create_index([("pickup_hour",   pymongo.ASCENDING)])
    db["clean_trips"].create_index([("pickup_date",   pymongo.ASCENDING)])
    db["clean_trips"].create_index([("pickup_year",   pymongo.ASCENDING)])

    db["clean_zones"].create_index([("LocationID", pymongo.ASCENDING)], unique=True)

    db["gold_zone_revenue"].create_index([("location_id",   pymongo.ASCENDING)], unique=True)
    db["gold_zone_revenue"].create_index([("total_revenue", pymongo.DESCENDING)])

    db["gold_top_routes"].create_index(
        [("pu_location_id", pymongo.ASCENDING),
         ("do_location_id", pymongo.ASCENDING)],
        unique=True,
        sparse=True,  # skip docs where either field is null (avoids dup key on stale data)
    )

    logger.info("MongoDB indexes ensured")


def upsert_clean_zones(db: Database, records: list[dict]) -> dict:
    if not records:
        return {"upserted": 0, "modified": 0}

    ops = [
        ReplaceOne({"LocationID": doc["LocationID"]}, doc, upsert=True)
        for doc in records
    ]
    result = db["clean_zones"].bulk_write(ops, ordered=False)
    summary = {"upserted": result.upserted_count, "modified": result.modified_count}
    logger.info(f"clean_zones upserted: {summary}")
    return summary


def load_clean_trips(
    db: Database,
    record_batches: Generator[list[dict], None, None],
    force: bool = False,
) -> int:
    """
    Bulk-loads clean_trips from the DuckDB batch generator.
    Skips if the collection already has data (unless force=True).
    Uses delete_many before inserting to preserve indexes.
    Returns number of records inserted (0 if skipped).
    """
    collection = db["clean_trips"]
    existing_count = collection.count_documents({})

    if existing_count > 0 and not force:
        logger.info(f"clean_trips has {existing_count:,} docs — skipping. Use force=True to reload.")
        return 0

    if existing_count > 0:
        logger.info(f"Removing {existing_count:,} existing clean_trips docs (indexes preserved)")
        collection.delete_many({})

    total_inserted = 0
    for batch in record_batches:
        clean = sanitize_records(batch)
        collection.insert_many(clean, ordered=False)
        total_inserted += len(clean)
        if total_inserted % 500_000 == 0:
            logger.info(f"  clean_trips: {total_inserted:,} inserted so far")

    logger.info(f"clean_trips load done: {total_inserted:,} documents")
    return total_inserted


def upsert_gold_zone_revenue(db: Database, records: list[dict]) -> dict:
    return _bulk_upsert(db["gold_zone_revenue"], records, ["location_id"], "gold_zone_revenue")


def upsert_gold_hourly_demand(db: Database, records: list[dict]) -> dict:
    return _bulk_upsert(db["gold_hourly_demand"], records, ["pickup_hour"], "gold_hourly_demand")


def upsert_gold_top_routes(db: Database, records: list[dict]) -> dict:
    return _bulk_upsert(
        db["gold_top_routes"], records,
        ["pu_location_id", "do_location_id"],
        "gold_top_routes",
    )


def record_pipeline_run(db: Database, run_metadata: dict) -> None:
    run_metadata["recorded_at"] = datetime.now(timezone.utc)
    db["pipeline_runs"].insert_one(run_metadata)
    logger.info("Run recorded in pipeline_runs")


def _bulk_upsert(collection, records: list[dict], key_fields: list[str], label: str) -> dict:
    if not records:
        return {"upserted": 0, "modified": 0}

    ops = [
        ReplaceOne({f: doc[f] for f in key_fields}, doc, upsert=True)
        for doc in records
    ]

    total_upserted = 0
    total_modified = 0
    for i in range(0, len(ops), MONGO_UPSERT_BATCH_SIZE):
        result = collection.bulk_write(ops[i : i + MONGO_UPSERT_BATCH_SIZE], ordered=False)
        total_upserted += result.upserted_count
        total_modified += result.modified_count

    summary = {"upserted": total_upserted, "modified": total_modified}
    logger.info(f"{label} upserted: {summary}")
    return summary


def sanitize_records(records: list[dict]) -> list[dict]:
    """Converts numpy/pandas scalars to Python natives for BSON compatibility."""
    def _convert(value):
        if isinstance(value, np.integer):
            return int(value)
        if isinstance(value, np.floating):
            return float(value)
        if isinstance(value, np.bool_):
            return bool(value)
        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        return value

    return [{k: _convert(v) for k, v in record.items()} for record in records]
