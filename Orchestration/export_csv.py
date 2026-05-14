#!/usr/bin/env python3
"""
Export gold-layer MongoDB collections to CSV files in data_product/.

Usage:
    python Orchestration/export_csv.py

Requires MongoDB to be running with bgd_taxidb populated.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
import config
from mongodb_loader import get_database


def main() -> None:
    db = get_database()
    out_dir = config.PROJECT_ROOT / "data_product"
    out_dir.mkdir(exist_ok=True)

    exports = {
        "gold_zone_revenue":  out_dir / "gold_zone_revenue.csv",
        "gold_hourly_demand": out_dir / "gold_hourly_demand.csv",
        "gold_top_routes":    out_dir / "gold_top_routes.csv",
        "clean_zones":        out_dir / "zone_lookup.csv",
    }

    for collection, path in exports.items():
        docs = list(db[collection].find({}, {"_id": 0}))
        if not docs:
            print(f"  WARNING: {collection} is empty — skipping")
            continue
        pd.DataFrame(docs).to_csv(path, index=False)
        print(f"  {collection}: {len(docs)} rows → {path}")


if __name__ == "__main__":
    main()
