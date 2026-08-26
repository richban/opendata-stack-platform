"""Iceberg and Spark table management utilities."""

import logging

from collections.abc import Iterable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from streamify.defs.resources import ClickHouseResource
from streamify.schemas import BRONZE_SCHEMAS, DLQ_SCHEMA

logger = logging.getLogger(__name__)


def create_namespace_if_not_exists(
    spark: SparkSession,
    catalog: str,
    namespace: str,
) -> None:
    """Create Iceberg namespace if it doesn't exist."""
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")
    except Exception as e:
        logger.debug("Namespace creation skipped or already exists: %s", e)


def create_table_if_not_exists(  # noqa: PLR0913
    spark: SparkSession,
    table_name: str,
    schema: StructType,
    *,
    catalog: str | None = None,
    namespace: str | None = None,
    partition_col: str = "event_date",
) -> None:
    """Create Iceberg table if it doesn't exist using Spark Catalog API.

    Supports both session-agnostic table names and fully qualified 3-part names.
    """
    target_table = (
        f"{catalog}.{namespace}.{table_name}" if catalog and namespace else table_name
    )

    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {target_table} ({schema.toDDL()}) "
        f"USING iceberg PARTITIONED BY ({partition_col})"
    )
    logger.info("Table created: %s", target_table)


# ---------------------------------------------------------------------------
# ClickHouse DDL bootstrap
# ---------------------------------------------------------------------------


def ensure_clickhouse_table_exists(clickhouse: ClickHouseResource) -> None:
    """Create ClickHouse database and ``ReplacingMergeTree`` table if absent."""
    logger.info(
        "Ensuring ClickHouse table '%s.silver_playback_events' exists...",
        clickhouse.database,
    )
    client = clickhouse.get_client()
    try:
        client.command(f"CREATE DATABASE IF NOT EXISTS {clickhouse.database}")
        logger.info("✓ ClickHouse database '%s' ensured.", clickhouse.database)

        client.command(f"""
            CREATE TABLE IF NOT EXISTS
            {clickhouse.database}.silver_playback_events (
                event_id String,
                user_id UInt64,
                artist String,
                song String,
                duration Float64,
                event_ts DateTime64(3),
                session_id String,
                city String,
                state String,
                enriched_first_name String,
                enriched_last_name String,
                enriched_gender String,
                enriched_city String,
                enriched_state String,
                enriched_zip String,
                song_year String,
                artist_location String,
                _processing_time DateTime64(3)
            ) ENGINE = ReplacingMergeTree(event_ts)
            ORDER BY (state, toYYYYMMDD(event_ts), event_id)
            SETTINGS index_granularity = 8192
        """)
        logger.info("✓ ClickHouse table 'silver_playback_events' ensured.")
    finally:
        client.close()


def bootstrap_storage(
    spark: SparkSession,
    clickhouse: ClickHouseResource,
    topics: Iterable[str],
    catalog: str,
    namespace: str,
) -> None:
    """Idempotently bootstrap all Iceberg and ClickHouse tables and namespaces."""
    # 1. ClickHouse DDL
    ensure_clickhouse_table_exists(clickhouse)

    # 2. Iceberg Bronze Tables
    for topic in topics:
        if topic not in BRONZE_SCHEMAS:
            raise ValueError(f"Schema not registered for topic '{topic}'")
        create_table_if_not_exists(
            spark=spark,
            table_name=f"bronze_{topic}",
            schema=BRONZE_SCHEMAS[topic],
            catalog=catalog,
            namespace=namespace,
            partition_col="event_date",
        )

    # 3. Iceberg DLQ Table
    create_table_if_not_exists(
        spark=spark,
        table_name="dlq_events_ingestion",
        schema=DLQ_SCHEMA,
        catalog=catalog,
        namespace=namespace,
        partition_col="_processing_date",
    )
    logger.info("✓ All Iceberg and ClickHouse storage bootstrapped successfully.")
