"""Lightweight client factories for Spark executor worker processes.

This module MUST NOT import Dagster, Pydantic, or any orchestrator dependencies,
ensuring Spark worker nodes remain clean and lightweight.
"""

from functools import cache

import clickhouse_connect
import redis


@cache
def get_executor_redis_client(host: str, port: int) -> redis.Redis:  # type: ignore[type-arg]
    """Return a cached Redis client for executor use.

    ``@cache`` (thread-safe on CPython) ensures a single client instance is
    reused across micro-batches in the same Python worker process.
    """
    return redis.Redis(host=host, port=port, decode_responses=True)


@cache
def get_executor_clickhouse_client(
    host: str,
    port: int,
    username: str,
    password: str,
    database: str,
) -> clickhouse_connect.driver.Client:
    """Return a cached clickhouse-connect client for executor use.

    Keyed on connection parameters so the client is reused across micro-batches
    in the same Python worker process without re-establishing connections.
    """
    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
    )
