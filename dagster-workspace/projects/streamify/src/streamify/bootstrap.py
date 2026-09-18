"""Iceberg and Spark table management utilities."""

import logging

from collections.abc import Iterable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from streamify.constants import (
    CLICKHOUSE_PLAYBACK_EVENTS_TABLE,
    DLQ_TABLE,
    QUARANTINE_TABLE,
)
from streamify.defs.resources import ClickHouseResource
from streamify.schemas import (
    BRONZE_SCHEMA,
    DLQ_SCHEMA,
    QUARANTINE_SCHEMA,
    SILVER_SCHEMAS,
)

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


def qualify_table(table_name: str, catalog: str | None, namespace: str | None) -> str:
    return f"{catalog}.{namespace}.{table_name}" if catalog and namespace else table_name


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
    target_table = qualify_table(table_name, catalog, namespace)

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
    table_name = CLICKHOUSE_PLAYBACK_EVENTS_TABLE
    logger.info(
        "Ensuring ClickHouse table '%s.%s' exists...",
        clickhouse.database,
        table_name,
    )
    client = clickhouse.get_client()
    try:
        client.command(f"CREATE DATABASE IF NOT EXISTS {clickhouse.database}")
        logger.info("✓ ClickHouse database '%s' ensured.", clickhouse.database)

        client.command(f"""
            CREATE TABLE IF NOT EXISTS
            {clickhouse.database}.{table_name} (
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
        logger.info("✓ ClickHouse table '%s' ensured.", table_name)
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

    # 2. Iceberg Bronze (raw, schema-on-read) + Silver Tables
    for topic in topics:
        if topic not in SILVER_SCHEMAS:
            raise ValueError(f"Schema not registered for topic '{topic}'")
        create_table_if_not_exists(
            spark=spark,
            table_name=f"bronze_{topic}",
            schema=BRONZE_SCHEMA,
            catalog=catalog,
            namespace=namespace,
            partition_col="_ingest_date",
        )
        create_table_if_not_exists(
            spark=spark,
            table_name=f"silver_{topic}",
            schema=SILVER_SCHEMAS[topic],
            catalog=catalog,
            namespace=namespace,
            partition_col="event_date",
        )

    # 3. Iceberg DLQ Table
    create_table_if_not_exists(
        spark=spark,
        table_name=DLQ_TABLE,
        schema=DLQ_SCHEMA,
        catalog=catalog,
        namespace=namespace,
        partition_col="_processing_date",
    )

    # 4. Iceberg schema-drift quarantine (retryable, unlike the DLQ)
    create_table_if_not_exists(
        spark=spark,
        table_name=QUARANTINE_TABLE,
        schema=QUARANTINE_SCHEMA,
        catalog=catalog,
        namespace=namespace,
        partition_col="_processing_date",
    )
    logger.info("✓ All Iceberg and ClickHouse storage bootstrapped successfully.")
