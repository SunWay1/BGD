# NYC Taxi ETL — Architecture

24 monthly NYC Yellow Taxi parquet files (2022-2023, ~1.2 GB) processed through
a Medallion Architecture (Raw → Clean → Gold).

| Component | Technology |
| --------- | ---------- |
| Orchestration | [Prefect 3](https://docs.prefect.io/) — tasks, retries, logging |
| Message Queue | [Redis Streams](https://redis.io/docs/data-types/streams/) — decouples data ingestion from storage |
| Transformations | [dbt](https://docs.getdbt.com/) + [DuckDB](https://duckdb.org/) — SQL models over parquet |
| Storage | MongoDB — all three layers |

---

## Pipeline Flow

```mermaid
flowchart TD
    SRC["Source Files"]

    PREFECT["Prefect\norchestrates all tasks"]

    subgraph STAGE1["① Raw — load into MongoDB"]
        direction LR
        Q_RAW["Redis Streams\nstream:raw:trips\nstream:raw:zones\n─────────────────\ntransient buffer\nfile-by-file"]
        MONGO_RAW["MongoDB · bgd_taxidb\n─────────────────\nraw_trips\nraw_zones"]
        Q_RAW -->|consume| MONGO_RAW
    end

    subgraph STAGE2["② Transform — DuckDB + dbt"]
        direction LR
        DDB_SRC["DuckDB\n─────────────────\nraw_trips  VIEW\nraw_zones  TABLE"]
        DBT["dbt models\n─────────────────\nclean/\ngold/"]
        DDB_OUT["DuckDB output\n─────────────────\nclean_trips\nclean_zones\ngold_zone_revenue\ngold_hourly_demand\ngold_top_routes"]
        DDB_SRC --> DBT --> DDB_OUT
    end

    subgraph STAGE3["③ Load — Clean & Gold into MongoDB"]
        direction LR
        Q_PROC["Redis Streams\nstream:clean:*\nstream:gold:*\n─────────────────\ntransient buffer\nbatch-by-batch"]
        MONGO_CG["MongoDB · bgd_taxidb\n─────────────────\nclean_trips · clean_zones\ngold_zone_revenue\ngold_hourly_demand\ngold_top_routes"]
        Q_PROC -->|consume| MONGO_CG
    end

    SRC -->|"publish  (file-by-file)"| Q_RAW
    SRC -->|"read_parquet glob\n(zero-copy)"| DDB_SRC
    DDB_OUT -->|publish| Q_PROC

    PREFECT -.->|controls| STAGE1
    PREFECT -.->|controls| STAGE2
    PREFECT -.->|controls| STAGE3
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
