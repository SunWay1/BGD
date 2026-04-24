"""
redis_queue.py — Redis Streams wrapper for the ETL pipeline.

Producers publish batches of records as JSON entries.
Consumers read, ACK, and delete each entry to keep memory usage low.
"""

import json
import logging
import math
import os
from datetime import datetime

import numpy as np
import redis

logger = logging.getLogger(__name__)

REDIS_URL      = os.getenv("REDIS_URL", "redis://localhost:6379")
CONSUMER_GROUP = "etl-workers"
CONSUMER_NAME  = "pipeline-worker"

STREAM_RAW_TRIPS    = "stream:raw:trips"
STREAM_RAW_ZONES    = "stream:raw:zones"
STREAM_CLEAN_TRIPS  = "stream:clean:trips"
STREAM_CLEAN_ZONES  = "stream:clean:zones"
STREAM_GOLD_REVENUE = "stream:gold:zone_revenue"
STREAM_GOLD_HOURLY  = "stream:gold:hourly_demand"
STREAM_GOLD_ROUTES  = "stream:gold:top_routes"


def get_client() -> redis.Redis:
    return redis.from_url(REDIS_URL, decode_responses=True)


class _Encoder(json.JSONEncoder):
    """Handles types that come out of pandas/DuckDB that stdlib json can't serialize."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return {"__dt__": obj.isoformat()}
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            # nan/inf are valid numpy but not valid JSON
            return None if math.isnan(obj) else float(obj)
        if isinstance(obj, np.bool_):
            return bool(obj)
        return super().default(obj)


def _decode_hook(obj: dict):
    if "__dt__" in obj:
        return datetime.fromisoformat(obj["__dt__"])
    return obj


def _ensure_group(client: redis.Redis, stream: str) -> None:
    try:
        client.xgroup_create(stream, CONSUMER_GROUP, id="0", mkstream=True)
    except redis.exceptions.ResponseError:
        pass  # group already exists


def publish(client: redis.Redis, stream: str, records: list[dict], batch_size: int = 1_000) -> int:
    """
    Pushes records to a Redis Stream in chunks of batch_size.
    Each stream entry holds a JSON-serialized list of records.
    Returns total number of records published.
    """
    total = 0
    for i in range(0, len(records), batch_size):
        chunk = records[i : i + batch_size]
        payload = json.dumps(chunk, cls=_Encoder)
        client.xadd(stream, {"payload": payload})
        total += len(chunk)
    return total


def consume(client: redis.Redis, stream: str, read_batch: int = 20):
    """
    Yields individual record dicts from the consumer group.
    ACKs and deletes each stream entry after all its records have been yielded,
    which keeps the stream from growing unbounded.
    """
    _ensure_group(client, stream)
    while True:
        response = client.xreadgroup(
            CONSUMER_GROUP, CONSUMER_NAME,
            {stream: ">"}, count=read_batch, block=3_000,
        )
        if not response:
            break
        for _, messages in response:
            for msg_id, data in messages:
                records = json.loads(data["payload"], object_hook=_decode_hook)
                yield from records
                client.xack(stream, CONSUMER_GROUP, msg_id)
                client.xdel(stream, msg_id)


def flush(client: redis.Redis, stream: str) -> None:
    """Deletes the entire stream. Used to clean up between pipeline runs."""
    client.delete(stream)
