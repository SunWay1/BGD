"""
load_raw_data.py — reads raw source files and publishes them to Redis Streams.

  - taxi_zone_lookup.csv         -> stream:raw:zones
  - yellow_tripdata_*.parquet    -> stream:raw:trips

The pipeline consumer task reads from those streams and writes to MongoDB.
"""

import csv
import glob
import os
import pandas as pd
import redis
from pathlib import Path

import redis_queue

DATA_DIR   = Path(os.getenv("RAW_DATA_DIR", str(Path(__file__).parent / "data" / "raw")))
BATCH_SIZE = 10_000  # records per Redis stream entry


def publish_zones(client: redis.Redis) -> int:
    csv_path = DATA_DIR / "taxi_zone_lookup.csv"
    if not csv_path.exists():
        print(f"ERROR: {csv_path} not found")
        return 0

    with open(csv_path, newline="", encoding="utf-8") as f:
        records = list(csv.DictReader(f))

    for row in records:
        row["LocationID"] = int(row["LocationID"])

    count = redis_queue.publish(client, redis_queue.STREAM_RAW_ZONES, records)
    print(f"raw_zones: {count} zones published to queue")
    return count


def publish_file(client: redis.Redis, filepath: Path) -> int:
    """Publishes a single parquet file to the raw trips stream."""
    filename = filepath.name
    df = pd.read_parquet(filepath)
    df["__sourceFile"] = filename

    for col_name in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col_name]):
            df[col_name] = pd.to_datetime(df[col_name], utc=False).dt.to_pydatetime()

    records = df.to_dict("records")
    return redis_queue.publish(client, redis_queue.STREAM_RAW_TRIPS, records, batch_size=BATCH_SIZE)


def publish_trips(client: redis.Redis) -> int:
    """Publishes all parquet files from DATA_DIR to the raw trips stream."""
    files = sorted(DATA_DIR.glob("yellow_tripdata_*.parquet"))
    if not files:
        print(f"ERROR: no parquet files found in {DATA_DIR}")
        return 0

    total = 0
    for filepath in files:
        count = publish_file(client, filepath)
        print(f"  {filepath.name}: {count} records published")
        total += count
    print(f"raw_trips: {total} total records published to queue")
    return total


if __name__ == "__main__":
    client = redis_queue.get_client()

    print("--- Zones (CSV) ---")
    publish_zones(client)

    print("\n--- Trips (Parquet) ---")
    publish_trips(client)
