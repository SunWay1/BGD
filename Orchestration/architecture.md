# NYC Taxi ETL — Architecture

24 monthly NYC Yellow Taxi parquet files (2022-2023, ~1.2 GB) processed through
a Medallion Architecture (Raw → Clean → Gold).

| Component | Technology |
| --------- | ---------- |
| Orchestration | [Prefect 3](https://docs.prefect.io/) — tasks, retries, logging |
| Transformations | [dbt](https://docs.getdbt.com/) + [DuckDB](https://duckdb.org/) — SQL models over parquet |
| Storage | MongoDB — all three layers |

---

## Pipeline Flow

```mermaid
flowchart TD
    subgraph SRC["Source Data  (ETL_task1/data/raw/)"]
        P["yellow_tripdata_YYYY-MM.parquet\n24 monthly files"]
        Z["taxi_zone_lookup.csv\n265 NYC taxi zones"]
    end

    subgraph ORCH["Prefect  (Orchestration/pipeline.py)"]
        direction TB
        T1["① validate_environment"]
        T2["② load_raw_to_mongodb"]
        T3["③ ensure_mongodb_indexes"]
        T4["④ create_duckdb_source_views"]
        T5["⑤ run_dbt_transformations"]
        T6["⑥ run_dbt_tests"]
        T7["⑦ check_retention_rate"]
        T8["⑧ load_processed_to_mongodb"]
        T9["⑨ record_pipeline_run"]

        T1 --> T2 --> T3 --> T4 --> T5 --> T6 --> T7 --> T8 --> T9
    end

    subgraph DBT["dbt models  (Orchestration/dbt_project/models/)"]
        direction TB
        subgraph CLEAN["clean/"]
            CZ["clean_zones.sql\nEWR fix, is_airport flag"]
            CT["clean_trips.sql\nfilters + derived fields"]
        end
        subgraph GOLD["gold/"]
            GZR["gold_zone_revenue.sql"]
            GHD["gold_hourly_demand.sql"]
            GTR["gold_top_routes.sql"]
        end

        CZ --> GZR
        CZ --> GTR
        CT --> GZR
        CT --> GHD
        CT --> GTR
    end

    subgraph DUCK["DuckDB  (taxi_etl.duckdb)"]
        RTV["raw_trips VIEW\nover parquet files"]
        RTB["raw_zones TABLE\nfrom CSV"]
    end

    subgraph MONGO["MongoDB  (bgd_taxidb)"]
        direction TB
        subgraph RAW["Raw Layer"]
            MRT["raw_trips"]
            MRZ["raw_zones"]
        end
        subgraph CLN["Clean Layer"]
            MCZ["clean_zones\nupsert by LocationID"]
            MCT["clean_trips\nbulk insert (guarded)"]
        end
        subgraph GLD["Gold Layer"]
            MGZR["gold_zone_revenue\nupsert by location_id"]
            MGHD["gold_hourly_demand\nupsert by pickup_hour"]
            MGTR["gold_top_routes\nupsert by (pu,do)_location_id"]
        end
        MPR["pipeline_runs, audit log"]
    end

    P -- "read_parquet()" --> RTV
    Z -- "read_csv_auto()" --> RTB
    RTV -- "source('raw','raw_trips')" --> CT
    RTB -- "source('raw','raw_zones')" --> CZ

    P -- "subprocess: load_data.py" --> MRT
    Z -- "subprocess: load_data.py" --> MRZ

    CZ -- "upsert" --> MCZ
    CT -- "bulk insert" --> MCT
    GZR -- "upsert" --> MGZR
    GHD -- "upsert" --> MGHD
    GTR -- "upsert" --> MGTR
    T9 -- "insert" --> MPR
```

---

## Idempotency

| Collection / Layer | Strategy |
| ------------------ | -------- |
| `raw_trips`, `raw_zones` | Skipped if populated (`--force` to reload) |
| dbt models (DuckDB) | Skipped if all five tables exist; `--force` → `dbt run --full-refresh` |
| `clean_trips` (MongoDB) | Skipped if populated; `--force` → delete_many + re-insert (indexes preserved) |
| `clean_zones` | Upsert by `LocationID` |
| `gold_zone_revenue` | Upsert by `location_id` |
| `gold_hourly_demand` | Upsert by `pickup_hour` |
| `gold_top_routes` | Upsert by `(pu_location_id, do_location_id)` |
| `pipeline_runs` | Append-only |
