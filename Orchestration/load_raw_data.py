"""
load_raw_data.py — loads raw source data into MongoDB:
  - taxi_zone_lookup.csv    -> raw_zones
  - yellow_tripdata_*.parquet -> raw_trips
"""

import csv
import glob
import os
import pandas as pd
import pymongo
from pathlib import Path

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.getenv("MONGO_DB_NAME", "bgd_taxidb")
DATA_DIR  = Path(os.getenv("RAW_DATA_DIR", str(Path(__file__).parent / "data" / "raw")))
BATCH_SIZE = 10_000


def load_zones(db):
    csv_path = DATA_DIR / "taxi_zone_lookup.csv"
    if not csv_path.exists():
        print(f"ERROR: {csv_path} not found")
        return

    db["raw_zones"].drop()

    with open(csv_path, newline="", encoding="utf-8") as f:
        records = list(csv.DictReader(f))

    for row in records:
        row["LocationID"] = int(row["LocationID"])

    db["raw_zones"].insert_many(records)
    print(f"raw_zones: {len(records)} zones loaded")


def load_trips(db):
    parquet_files = sorted(glob.glob(str(DATA_DIR / "yellow_tripdata_*.parquet")))
    if not parquet_files:
        print("No parquet files found in", DATA_DIR)
        return

    print(f"Found {len(parquet_files)} parquet files")
    col = db["raw_trips"]
    total = 0

    for filepath in parquet_files:
        filename = Path(filepath).name
        print(f"\nLoading {filename}...")

        df = pd.read_parquet(filepath)
        df["__sourceFile"] = filename

        for col_name in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col_name]):
                df[col_name] = pd.to_datetime(df[col_name], utc=False).dt.to_pydatetime()

        records = df.to_dict("records")

        for i in range(0, len(records), BATCH_SIZE):
            col.insert_many(records[i : i + BATCH_SIZE], ordered=False)

        total += len(records)
        print(f"  {len(records):,} records")

    print(f"\nTotal loaded: {total:,} documents")
    print(f"raw_trips count: {col.count_documents({}):,}")


if __name__ == "__main__":
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]

    print("--- Zones (CSV) ---")
    load_zones(db)

    print("\n--- Trips (Parquet) ---")
    load_trips(db)

    client.close()
