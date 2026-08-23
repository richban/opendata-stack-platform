"""Iceberg and Spark table management utilities."""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from streamify.defs.resources import (
    StreamingJobConfig,
    create_clickhouse_resource,
    create_spark_session,
    get_streaming_config,
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


def create_table_if_not_exists(
    spark: SparkSession,
    table_name: str,
    schema: StructType,
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

    try:
        if spark.catalog.tableExists(target_table):
            return
    except Exception as e:
        logger.debug("Table existence check bypassed: %s", e)

    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {target_table} ({schema.toDDL()}) "
        f"USING iceberg PARTITIONED BY ({partition_col})"
    )


# ---------------------------------------------------------------------------
# ClickHouse DDL bootstrap
# ---------------------------------------------------------------------------


def ensure_clickhouse_table_exists(config: StreamingJobConfig) -> None:
    """Create ClickHouse database and ``ReplacingMergeTree`` table if absent."""
    logger.info(
        "Ensuring ClickHouse table '%s.silver_playback_events' exists (host=%s:%d)...",
        config.clickhouse_db,
        config.clickhouse_host,
        config.clickhouse_port,
    )

    client = create_clickhouse_resource(config)
    try:
        client.command(f"CREATE DATABASE IF NOT EXISTS {config.clickhouse_db}")
        logger.info("✓ ClickHouse database '%s' ensured.", config.clickhouse_db)

        client.command(f"""
            CREATE TABLE IF NOT EXISTS
            {config.clickhouse_db}.silver_playback_events (
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
