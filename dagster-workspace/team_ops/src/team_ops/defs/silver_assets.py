"""Dagster Silver layer assets for deduplicated event data.

This module contains batch assets that read from Bronze tables,
perform deduplication, and write to Silver tables for downstream analytics.
"""


import dagster as dg

from dagster import AssetKey, DailyPartitionsDefinition
from pyspark.sql.functions import (
    avg,
    col,
    concat_ws,
    count as spark_count,
    first,
    lag,
    lit,
    max,
    min,
    row_number,
    sha2,
    sum as spark_sum,
    unix_timestamp,
    when,
)
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

from team_ops.defs.resources import StreamingJobConfig


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
    spark: dg.ResourceParam[SparkSession],
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

    session = spark
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
    spark: dg.ResourceParam[SparkSession],
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

    session = spark
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


@dg.asset(
    group_name="silver",
    kinds={"spark", "iceberg"},
    owners=["team:team-ops"],
    tags={"layer": "silver", "topic": "auth_events"},
    deps=[AssetKey("bronze_streaming_job")],
    partitions_def=DailyPartitionsDefinition(start_date="2024-01-01"),
    description="Deduplicated auth events from Bronze to Silver layer",
)
def silver_auth_events(
    context: dg.AssetExecutionContext,
    spark: dg.ResourceParam[SparkSession],
    streaming_config: StreamingJobConfig,
) -> dg.MaterializeResult:
    """Deduplicate bronze_auth_events and write to silver_auth_events.

    This asset reads from the Bronze auth events table, removes duplicates
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

    session = spark
    catalog = streaming_config.catalog
    namespace = streaming_config.namespace

    source_table = f"{catalog}.{namespace}.bronze_auth_events"
    target_table = f"{catalog}.{namespace}.silver_auth_events"

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
    deps=[AssetKey("silver_listen_events"), AssetKey("silver_page_view_events")],
    description="Reconstructed user sessions from listen and page view events",
)
def silver_user_sessions(
    context: dg.AssetExecutionContext,
    spark: dg.ResourceParam[SparkSession],
    streaming_config: StreamingJobConfig,
) -> dg.MaterializeResult:
    """Reconstruct user sessions from Silver layer events.

    This asset reads from silver_listen_events and silver_page_view_events,
    reconstructs user sessions using a 30-minute inactivity timeout, and
    writes session-level metrics to silver_user_sessions.

    Session Reconstruction Logic:
    - Combines listen_events and page_view_events by userId
    - Orders events by timestamp for each user
    - Uses lag() to calculate time gaps between consecutive events
    - New session starts when gap > 30 minutes (1800 seconds)
    - Session ID generated as SHA-256 hash of userId|session_start_ts

    Output Schema:
    - session_id: SHA-256 hash of userId_session_start_ts
    - user_id: User identifier
    - session_start: First event timestamp in session
    - session_end: Last event timestamp in session
    - session_duration_seconds: Duration of session in seconds
    - track_count: Number of listen events in session
    - page_count: Number of page view events in session
    - level: User level (free/paid)

    Args:
        context: Dagster asset execution context
        spark: SparkConnectResource for Spark operations
        streaming_config: Configuration containing catalog and namespace info

    Returns:
        MaterializeResult with metadata about sessions created and metrics
    """
    session = spark
    catalog = streaming_config.catalog
    namespace = streaming_config.namespace

    listen_table = f"{catalog}.{namespace}.silver_listen_events"
    page_view_table = f"{catalog}.{namespace}.silver_page_view_events"
    target_table = f"{catalog}.{namespace}.silver_user_sessions"

    context.log.info(f"Reading from Silver tables: {listen_table}, {page_view_table}")

    # Read from Silver tables
    df_listen = session.table(listen_table)
    df_page_view = session.table(page_view_table)

    # Select common fields and add event type indicator
    df_listen_events = df_listen.select(
        col("userId"),
        col("ts").alias("event_ts"),
        col("level"),
    ).withColumn("event_type", lit("listen"))

    df_page_view_events = df_page_view.select(
        col("userId"),
        col("ts").alias("event_ts"),
        col("level"),
    ).withColumn("event_type", lit("page_view"))

    # Union all events
    df_all_events = df_listen_events.unionByName(
        df_page_view_events, allowMissingColumns=True
    )

    # Add window for session boundary detection
    window_spec = Window.partitionBy("userId").orderBy("event_ts")

    # Calculate time gap from previous event
    df_with_lag = df_all_events.withColumn(
        "prev_event_ts", lag("event_ts", 1).over(window_spec)
    ).withColumn(
        "time_gap_seconds",
        when(col("prev_event_ts").isNull(), 0).otherwise(
            unix_timestamp("event_ts") - unix_timestamp("prev_event_ts")
        ),
    )

    # Identify new sessions: gap > 30 minutes (1800 seconds) or first event
    df_with_session_flag = df_with_lag.withColumn(
        "is_new_session", when(col("time_gap_seconds") > 1800, 1).otherwise(0)
    )

    # Create cumulative session number per user using sum() over window
    session_window = (
        Window.partitionBy("userId")
        .orderBy("event_ts")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    df_with_session_num = df_with_session_flag.withColumn(
        "session_num", spark_sum("is_new_session").over(session_window)
    )

    # Get session start timestamp for each session
    session_agg_window = Window.partitionBy("userId", "session_num")

    df_with_session_start = df_with_session_num.withColumn(
        "session_start_ts", first("event_ts").over(session_agg_window)
    )

    # Generate session ID using SHA-256 hash
    df_with_session_id = df_with_session_start.withColumn(
        "session_id", sha2(concat_ws("_", col("userId"), col("session_start_ts")), 256)
    )

    # Aggregate to session level
    df_sessions = (
        df_with_session_id.groupBy("session_id", "userId", "level")
        .agg(
            min("event_ts").alias("session_start"),
            max("event_ts").alias("session_end"),
            spark_count(when(col("event_type") == "listen", 1)).alias("track_count"),
            spark_count(when(col("event_type") == "page_view", 1)).alias("page_count"),
        )
        .withColumn(
            "session_duration_seconds",
            unix_timestamp("session_end") - unix_timestamp("session_start"),
        )
        .withColumn("session_date", col("session_start").cast("date"))
    )

    # Select final columns
    df_final = df_sessions.select(
        "session_id",
        col("userId").alias("user_id"),
        "session_start",
        "session_end",
        "session_duration_seconds",
        "track_count",
        "page_count",
        "level",
        "session_date",
    )

    # Get metrics before writing
    session_count = df_final.count()

    # Calculate aggregate metrics
    if session_count > 0:
        metrics_df = df_final.agg(
            avg("session_duration_seconds").alias("avg_duration"),
            avg("track_count").alias("avg_tracks"),
        ).collect()[0]

        avg_session_duration = (
            float(metrics_df["avg_duration"]) if metrics_df["avg_duration"] else 0.0
        )
        avg_tracks_per_session = (
            float(metrics_df["avg_tracks"]) if metrics_df["avg_tracks"] else 0.0
        )
    else:
        avg_session_duration = 0.0
        avg_tracks_per_session = 0.0

    context.log.info(f"Total sessions reconstructed: {session_count}")
    context.log.info(f"Average session duration: {avg_session_duration:.2f} seconds")
    context.log.info(f"Average tracks per session: {avg_tracks_per_session:.2f}")

    # Write to Silver table with dynamic partition overwrite
    context.log.info(f"Writing to Silver table: {target_table}")

    df_final.write.mode("overwrite").option(
        "partitionOverwriteMode", "dynamic"
    ).partitionBy("session_date").format("iceberg").saveAsTable(target_table)

    context.log.info(f"✓ Successfully wrote {session_count} sessions to {target_table}")

    return dg.MaterializeResult(
        metadata={
            "session_count": dg.MetadataValue.int(session_count),
            "avg_session_duration_seconds": dg.MetadataValue.float(avg_session_duration),
            "avg_tracks_per_session": dg.MetadataValue.float(avg_tracks_per_session),
            "source_tables": dg.MetadataValue.json(
                {"listen_events": listen_table, "page_view_events": page_view_table}
            ),
            "target_table": dg.MetadataValue.text(target_table),
        }
    )
