"""
Post-build assertions for the pipeline.

dbt schema tests (not_null, unique) in models/clean/schema.yml and
models/gold/schema.yml cover field-level checks for all dbt models.

The functions here cover things dbt can't easily express:
  - MongoDB raw collections populated check
  - Clean trip retention rate (ratio test across two tables)
"""

import logging

from config import MIN_CLEAN_TRIP_RETENTION_RATE

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    pass


def assert_raw_layer_loaded(raw_trips_count: int, raw_zones_count: int) -> None:
    if raw_trips_count == 0:
        raise DataQualityError(
            "raw_trips is empty — run load_data.py or the pipeline with force=True."
        )
    if raw_zones_count == 0:
        raise DataQualityError(
            "raw_zones is empty — check that taxi_zone_lookup.csv exists in data/raw/."
        )
    logger.info(f"[DQ] raw layer OK — trips: {raw_trips_count:,}, zones: {raw_zones_count}")


def assert_clean_trips_retention(raw_trip_count: int, clean_trip_count: int) -> None:
    """
    Checks that the clean layer didn't discard too many trips.
    Very low retention usually means a filter threshold in config.py / dbt_project.yml is too aggressive.
    """
    if clean_trip_count == 0:
        raise DataQualityError(
            "clean_trips is empty — check parquet files and filter thresholds in dbt_project.yml."
        )

    if raw_trip_count == 0:
        logger.warning("[DQ] Cannot compute retention rate — raw_trip_count is 0")
        return

    retention = clean_trip_count / raw_trip_count
    if retention < MIN_CLEAN_TRIP_RETENTION_RATE:
        raise DataQualityError(
            f"Clean trip retention is {retention:.1%}, expected >= {MIN_CLEAN_TRIP_RETENTION_RATE:.1%}. "
            f"Got {clean_trip_count:,} from {raw_trip_count:,} raw trips."
        )
    logger.info(f"[DQ] clean_trips OK — {clean_trip_count:,} rows ({retention:.1%} retention)")
