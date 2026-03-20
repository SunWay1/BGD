"""
load_data.py — ładuje dane źródłowe do MongoDB:
  - taxi_zone_lookup.csv  -> raw_zones
  - yellow_tripdata_*.parquet -> raw_trips
"""

import csv
import glob
import pandas as pd
import pymongo
from pathlib import Path

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "bgd_taxidb"
DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
BATCH_SIZE = 10_000


def load_zones(db):
    csv_path = DATA_DIR / "taxi_zone_lookup.csv"
    if not csv_path.exists():
        print(f"ERROR: {csv_path} nie znaleziony")
        return

    db["raw_zones"].drop()

    with open(csv_path, newline="", encoding="utf-8") as f:
        records = list(csv.DictReader(f))

    for row in records:
        row["LocationID"] = int(row["LocationID"])

    db["raw_zones"].insert_many(records)
    print(f"raw_zones: {len(records)} stref załadowanych")


def load_trips(db):
    parquet_files = sorted(glob.glob(str(DATA_DIR / "yellow_tripdata_*.parquet")))
    if not parquet_files:
        print("Brak plików parquet w", DATA_DIR)
        return

    print(f"Znaleziono {len(parquet_files)} plików parquet")
    col = db["raw_trips"]
    total = 0

    for filepath in parquet_files:
        filename = Path(filepath).name
        print(f"\nŁadowanie {filename}...")

        df = pd.read_parquet(filepath)
        df["__sourceFile"] = filename

        # MongoDB wymaga natywnych datetime, nie numpy
        for col_name in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col_name]):
                df[col_name] = pd.to_datetime(df[col_name], utc=False).dt.to_pydatetime()

        records = df.to_dict("records")

        for i in range(0, len(records), BATCH_SIZE):
            col.insert_many(records[i : i + BATCH_SIZE], ordered=False)

        total += len(records)
        print(f"  {len(records):,} rekordów")

    print(f"\nŁącznie załadowano: {total:,} dokumentów")
    print(f"raw_trips count: {col.count_documents({}):,}")


if __name__ == "__main__":
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]

    print("--- Strefy (CSV) ---")
    load_zones(db)

    print("\n--- Przejazdy (Parquet) ---")
    load_trips(db)

    client.close()
