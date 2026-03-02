"""Dagster Gold layer assets for analytics aggregations.

This module contains batch assets that read from Silver tables,
perform aggregations, and write to Gold tables for downstream analytics.
"""

from __future__ import annotations

import dagster as dg
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, countDistinct

from team_ops.defs.resources import StreamingJobConfig


@dg.asset(
    deps=[dg.AssetKey("silver_listen_events")],
    group_name="gold",
    kinds={"spark", "iceberg"},
    owners=["team:team-ops"],
    tags={"layer": "gold"},
    description="Daily top tracks aggregation with play counts and unique listeners",
)
def gold_top_tracks(
    context,
    spark: dg.ResourceParam[SparkSession],
    streaming_config: StreamingJobConfig,
) -> dg.MaterializeResult:
    """Aggregate daily top tracks metrics from silver_listen_events.

    This asset reads from silver_listen_events, groups by event_date, song, and artist,
    and computes aggregated metrics including play count, unique listeners, and average
    track duration.

    Aggregation Logic:
    - GROUP BY event_date, song, artist: Groups records by date and track
    - COUNT(*) as play_count: Total plays per track
    - COUNT(DISTINCT userId) as unique_listeners: Unique users per track
    - AVG(duration) as avg_duration: Average track duration

    Args:
        context: Dagster asset execution context for logging
        spark: SparkConnectResource for Spark operations
        streaming_config: Configuration containing catalog and namespace info

    Returns:
        MaterializeResult with metadata about output rows and event date
    """
    session = spark
    catalog = streaming_config.catalog
    namespace = streaming_config.namespace

    source_table = f"{catalog}.{namespace}.silver_listen_events"
    target_table = f"{catalog}.{namespace}.gold_top_tracks"

    context.log.info(f"Reading from Silver table: {source_table}")

    # Read from Silver table
    df = session.table(source_table)

    # Aggregate by event_date, song, artist
    df_aggregated = (
        df.groupBy("event_date", "song", "artist")
        .agg(
            count("*").alias("play_count"),
            countDistinct("userId").alias("unique_listeners"),
            avg("duration").alias("avg_duration"),
        )
        .select(
            col("event_date"),
            col("song"),
            col("artist"),
            col("play_count"),
            col("unique_listeners"),
            col("avg_duration"),
        )
    )

    output_rows = df_aggregated.count()
    context.log.info(f"Aggregated {output_rows} track records")

    # Get the event_date being processed (if partitioned, use partition key)
    event_date = context.partition_key if context.has_partition_key else "all_dates"
    context.log.info(f"Processing event_date: {event_date}")

    # Write to Gold table with dynamic partition overwrite
    context.log.info(f"Writing to Gold table: {target_table}")

    write_mode = "overwrite"
    partition_overwrite_mode = "dynamic"

    df_aggregated.write.mode(write_mode).option(
        "partitionOverwriteMode", partition_overwrite_mode
    ).partitionBy("event_date").format("iceberg").saveAsTable(target_table)

    context.log.info(f"✓ Successfully wrote {output_rows} rows to {target_table}")

    return dg.MaterializeResult(
        metadata={
            "output_rows": dg.MetadataValue.int(output_rows),
            "event_date": dg.MetadataValue.text(event_date),
            "source_table": dg.MetadataValue.text(source_table),
            "target_table": dg.MetadataValue.text(target_table),
        }
    )


@dg.asset(
    deps=[dg.AssetKey("silver_listen_events")],
    group_name="gold",
    kinds={"spark", "iceberg"},
    owners=["team:team-ops"],
    tags={"layer": "gold"},
    description="Daily top artists aggregation with play counts and unique listeners",
)
def gold_top_artists(
    context,
    spark: dg.ResourceParam[SparkSession],
    streaming_config: StreamingJobConfig,
) -> dg.MaterializeResult:
    """Aggregate daily top artists metrics from silver_listen_events.

    This asset reads from silver_listen_events, groups by event_date and artist,
    and computes aggregated metrics including play count and unique listeners.

    Aggregation Logic:
    - GROUP BY event_date, artist: Groups records by date and artist
    - COUNT(*) as play_count: Total plays per artist
    - COUNT(DISTINCT userId) as unique_listeners: Unique users per artist

    Args:
        context: Dagster asset execution context for logging
        spark: SparkConnectResource for Spark operations
        streaming_config: Configuration containing catalog and namespace info

    Returns:
        MaterializeResult with metadata about output rows and event date
    """
    session = spark
    catalog = streaming_config.catalog
    namespace = streaming_config.namespace

    source_table = f"{catalog}.{namespace}.silver_listen_events"
    target_table = f"{catalog}.{namespace}.gold_top_artists"

    context.log.info(f"Reading from Silver table: {source_table}")

    # Read from Silver table
    df = session.table(source_table)

    # Aggregate by event_date, artist
    df_aggregated = (
        df.groupBy("event_date", "artist")
        .agg(
            count("*").alias("play_count"),
            countDistinct("userId").alias("unique_listeners"),
        )
        .select(
            col("event_date"),
            col("artist"),
            col("play_count"),
            col("unique_listeners"),
        )
    )

    output_rows = df_aggregated.count()
    context.log.info(f"Aggregated {output_rows} artist records")

    # Get the event_date being processed (if partitioned, use partition key)
    event_date = context.partition_key if context.has_partition_key else "all_dates"
    context.log.info(f"Processing event_date: {event_date}")

    # Write to Gold table with dynamic partition overwrite
    context.log.info(f"Writing to Gold table: {target_table}")

    write_mode = "overwrite"
    partition_overwrite_mode = "dynamic"

    df_aggregated.write.mode(write_mode).option(
        "partitionOverwriteMode", partition_overwrite_mode
    ).partitionBy("event_date").format("iceberg").saveAsTable(target_table)

    context.log.info(f"✓ Successfully wrote {output_rows} rows to {target_table}")

    return dg.MaterializeResult(
        metadata={
            "output_rows": dg.MetadataValue.int(output_rows),
            "event_date": dg.MetadataValue.text(event_date),
            "source_table": dg.MetadataValue.text(source_table),
            "target_table": dg.MetadataValue.text(target_table),
        }
    )
