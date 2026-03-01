"""Dagster Silver layer assets for deduplicated event data.

This module contains batch assets that read from Bronze tables,
perform deduplication, and write to Silver tables for downstream analytics.
"""

from typing import Any

import dagster as dg
from dagster import AssetKey, DailyPartitionsDefinition
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

from team_ops.defs.resources import SparkConnectResource, StreamingJobConfig


@dg.asset(
    group_name="silver",
    kinds={"spark", "iceberg"},
    owners=["team:team-ops"],
    tags={"layer": "silver", "topic": "listen_events"},
    deps=[AssetKey("bronze_streaming_job")],
    partitions_def=DailyPartitionsDefinition(start_date="2024-01-01"),
    description="Deduplicated listen events from Bronze to Silver layer",
)
def silver_listen_events(
    context: dg.AssetExecutionContext,
    spark: SparkConnectResource,
    streaming_config: StreamingJobConfig,
) -> dg.MaterializeResult:
    """Deduplicate bronze_listen_events and write to silver_listen_events.

    This asset reads from the Bronze listen events table, removes duplicates
    by keeping only the latest record for each event_id (based on _processing_time),
    and writes the deduplicated data to the Silver layer.

    Deduplication Logic:
    - PARTITION BY event_id: Groups records by unique event identifier
    - ORDER BY _processing_time DESC: Most recent record first
    - Keep row_num = 1: Retain only the latest version of each event

    Args:
        context: Dagster asset execution context for logging and partition info
        spark: SparkConnectResource for Spark operations
        streaming_config: Configuration containing catalog and namespace info

    Returns:
        MaterializeResult with metadata about input/output rows and duplicates removed
    """
    partition_date = context.partition_key if context.has_partition_key else None

    session = spark.get_session()
    catalog = streaming_config.catalog
    namespace = streaming_config.namespace

    source_table = f"{catalog}.{namespace}.bronze_listen_events"
    target_table = f"{catalog}.{namespace}.silver_listen_events"

    context.log.info(f"Reading from Bronze table: {source_table}")
    if partition_date:
        context.log.info(f"Processing partition: {partition_date}")

    # Read from Bronze table
    df = session.table(source_table)

    # Apply partition filter if running for specific partition
    if partition_date:
        df = df.filter(col("event_date") == partition_date)

    # Count input rows
    input_rows = df.count()
    context.log.info(f"Input rows: {input_rows}")

    # Deduplicate using ROW_NUMBER() window function
    # Keep only the latest record for each event_id based on _processing_time
    window_spec = Window.partitionBy("event_id").orderBy(col("_processing_time").desc())

    df_deduped = (
        df.withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    output_rows = df_deduped.count()
    duplicate_rows_removed = input_rows - output_rows

    context.log.info(f"Output rows after deduplication: {output_rows}")
    context.log.info(f"Duplicate rows removed: {duplicate_rows_removed}")

    # Write to Silver table with dynamic partition overwrite
    context.log.info(f"Writing to Silver table: {target_table}")

    write_mode = "overwrite" if partition_date else "overwrite"
    partition_overwrite_mode = "dynamic" if partition_date else "dynamic"

    df_deduped.write.mode(write_mode).option(
        "partitionOverwriteMode", partition_overwrite_mode
    ).partitionBy("event_date").format("iceberg").saveAsTable(target_table)

    context.log.info(f"✓ Successfully wrote {output_rows} rows to {target_table}")

    return dg.MaterializeResult(
        metadata={
            "input_rows": dg.MetadataValue.int(input_rows),
            "output_rows": dg.MetadataValue.int(output_rows),
            "duplicate_rows_removed": dg.MetadataValue.int(duplicate_rows_removed),
            "source_table": dg.MetadataValue.text(source_table),
            "target_table": dg.MetadataValue.text(target_table),
            "partition_date": dg.MetadataValue.text(partition_date or "full_table"),
        }
    )


@dg.asset(
    group_name="silver",
    kinds={"spark", "iceberg"},
    owners=["team:team-ops"],
    tags={"layer": "silver", "topic": "page_view_events"},
    deps=[AssetKey("bronze_streaming_job")],
    partitions_def=DailyPartitionsDefinition(start_date="2024-01-01"),
    description="Deduplicated page view events from Bronze to Silver layer",
)
def silver_page_view_events(
    context: dg.AssetExecutionContext,
    spark: SparkConnectResource,
    streaming_config: StreamingJobConfig,
) -> dg.MaterializeResult:
    """Deduplicate bronze_page_view_events and write to silver_page_view_events.

    This asset reads from the Bronze page view events table, removes duplicates
    by keeping only the latest record for each event_id (based on _processing_time),
    and writes the deduplicated data to the Silver layer.

    Deduplication Logic:
    - PARTITION BY event_id: Groups records by unique event identifier
    - ORDER BY _processing_time DESC: Most recent record first
    - Keep row_num = 1: Retain only the latest version of each event

    Args:
        context: Dagster asset execution context for logging and partition info
        spark: SparkConnectResource for Spark operations
        streaming_config: Configuration containing catalog and namespace info

    Returns:
        MaterializeResult with metadata about input/output rows and duplicates removed
    """
    partition_date = context.partition_key if context.has_partition_key else None

    session = spark.get_session()
    catalog = streaming_config.catalog
    namespace = streaming_config.namespace

    source_table = f"{catalog}.{namespace}.bronze_page_view_events"
    target_table = f"{catalog}.{namespace}.silver_page_view_events"

    context.log.info(f"Reading from Bronze table: {source_table}")
    if partition_date:
        context.log.info(f"Processing partition: {partition_date}")

    # Read from Bronze table
    df = session.table(source_table)

    # Apply partition filter if running for specific partition
    if partition_date:
        df = df.filter(col("event_date") == partition_date)

    # Count input rows
    input_rows = df.count()
    context.log.info(f"Input rows: {input_rows}")

    # Deduplicate using ROW_NUMBER() window function
    # Keep only the latest record for each event_id based on _processing_time
    window_spec = Window.partitionBy("event_id").orderBy(col("_processing_time").desc())

    df_deduped = (
        df.withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    output_rows = df_deduped.count()
    duplicate_rows_removed = input_rows - output_rows

    context.log.info(f"Output rows after deduplication: {output_rows}")
    context.log.info(f"Duplicate rows removed: {duplicate_rows_removed}")

    # Write to Silver table with dynamic partition overwrite
    context.log.info(f"Writing to Silver table: {target_table}")

    write_mode = "overwrite" if partition_date else "overwrite"
    partition_overwrite_mode = "dynamic" if partition_date else "dynamic"

    df_deduped.write.mode(write_mode).option(
        "partitionOverwriteMode", partition_overwrite_mode
    ).partitionBy("event_date").format("iceberg").saveAsTable(target_table)

    context.log.info(f"✓ Successfully wrote {output_rows} rows to {target_table}")

    return dg.MaterializeResult(
        metadata={
            "input_rows": dg.MetadataValue.int(input_rows),
            "output_rows": dg.MetadataValue.int(output_rows),
            "duplicate_rows_removed": dg.MetadataValue.int(duplicate_rows_removed),
            "source_table": dg.MetadataValue.text(source_table),
            "target_table": dg.MetadataValue.text(target_table),
            "partition_date": dg.MetadataValue.text(partition_date or "full_table"),
        }
    )
