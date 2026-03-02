"""Dagster Gold layer assets for analytics aggregations.

This module contains batch assets that read from Silver tables,
perform aggregations, and write to Gold tables for downstream analytics.
"""

from __future__ import annotations

import dagster as dg
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, countDistinct, date_trunc, when

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


@dg.asset(
    deps=[dg.AssetKey("silver_listen_events")],
    group_name="gold",
    kinds={"spark", "iceberg"},
    owners=["team:team-ops"],
    tags={"layer": "gold"},
    description="Daily and Monthly Active Users metrics from silver_listen_events",
)
def gold_dau_mau(
    context,
    spark: dg.ResourceParam[SparkSession],
    streaming_config: StreamingJobConfig,
) -> dg.MaterializeResult:
    """Compute DAU and MAU metrics from silver_listen_events.

    This asset reads from silver_listen_events and computes two key engagement metrics:
    - DAU (Daily Active Users): Count of unique users per event_date
    - MAU (Monthly Active Users): Count of unique users per month (YYYY-MM)

    Aggregation Logic:
    - DAU: COUNT(DISTINCT userId) grouped by event_date
    - MAU: COUNT(DISTINCT userId) grouped by year_month (date_trunc of event_date)
    - Results are joined on year_month to provide both metrics in one view

    Args:
        context: Dagster asset execution context for logging
        spark: SparkConnectResource for Spark operations
        streaming_config: Configuration containing catalog and namespace info

    Returns:
        MaterializeResult with metadata about dau, mau, event_date, and mau_month
    """
    session = spark
    catalog = streaming_config.catalog
    namespace = streaming_config.namespace

    source_table = f"{catalog}.{namespace}.silver_listen_events"
    target_table = f"{catalog}.{namespace}.gold_dau_mau"

    context.log.info(f"Reading from Silver table: {source_table}")

    # Read from Silver table
    df = session.table(source_table)

    # Calculate DAU: Daily Active Users grouped by event_date
    dau_df = (
        df.groupBy("event_date")
        .agg(countDistinct("userId").alias("dau"))
        .select(col("event_date"), col("dau"))
    )

    # Calculate MAU: Monthly Active Users grouped by year_month
    # Use date_trunc to get YYYY-MM format
    mau_df = (
        df.withColumn("year_month", date_trunc("month", col("event_date")))
        .groupBy("year_month")
        .agg(countDistinct("userId").alias("mau"))
        .select(col("year_month"), col("mau"))
    )

    # Join DAU and MAU dataframes
    # Extract year_month from event_date in dau_df for joining
    dau_with_month = dau_df.withColumn(
        "year_month", date_trunc("month", col("event_date"))
    )
    result_df = dau_with_month.join(mau_df, "year_month", "left").select(
        col("event_date"),
        col("dau"),
        col("mau"),
        col("year_month"),
    )

    output_rows = result_df.count()
    context.log.info(f"Computed DAU/MAU for {output_rows} date records")

    # Get the event_date being processed (if partitioned, use partition key)
    event_date = context.partition_key if context.has_partition_key else "all_dates"
    context.log.info(f"Processing event_date: {event_date}")

    # Get MAU month for metadata (first year_month in results)
    mau_month_row = result_df.select("year_month").first()
    mau_month = str(mau_month_row[0]) if mau_month_row else "unknown"

    # Write to Gold table with dynamic partition overwrite
    context.log.info(f"Writing to Gold table: {target_table}")

    write_mode = "overwrite"
    partition_overwrite_mode = "dynamic"

    result_df.write.mode(write_mode).option(
        "partitionOverwriteMode", partition_overwrite_mode
    ).partitionBy("event_date").format("iceberg").saveAsTable(target_table)

    context.log.info(f"✓ Successfully wrote {output_rows} rows to {target_table}")

    return dg.MaterializeResult(
        metadata={
            "dau": dg.MetadataValue.int(
                dau_df.select("dau").first()[0] if output_rows > 0 else 0
            ),
            "mau": dg.MetadataValue.int(
                mau_df.select("mau").first()[0] if output_rows > 0 else 0
            ),
            "event_date": dg.MetadataValue.text(event_date),
            "mau_month": dg.MetadataValue.text(mau_month),
            "source_table": dg.MetadataValue.text(source_table),
            "target_table": dg.MetadataValue.text(target_table),
            "output_rows": dg.MetadataValue.int(output_rows),
        }
    )


@dg.asset(
    deps=[
        dg.AssetKey("silver_auth_events"),
        dg.AssetKey("silver_listen_events"),
    ],
    group_name="gold",
    kinds={"spark", "iceberg"},
    owners=["team:team-ops"],
    tags={"layer": "gold"},
    description="User conversion funnel from free to paid subscriptions",
)
def gold_user_conversion_funnel(
    context,
    spark: dg.ResourceParam[SparkSession],
    streaming_config: StreamingJobConfig,
) -> dg.MaterializeResult:
    """Compute user conversion funnel metrics from Silver tables.

    This asset reads from silver_auth_events and silver_listen_events to compute
    conversion funnel metrics: free users, paid users, and conversion rate.

    Funnel Logic:
    - free_users: COUNT(DISTINCT userId) WHERE level='free' per event_date
    - paid_users: COUNT(DISTINCT userId) WHERE level='paid' per event_date
    - conversion_rate: (paid_users / free_users) * 100 (null-safe)

    Args:
        context: Dagster asset execution context for logging
        spark: SparkConnectResource for Spark operations
        streaming_config: Configuration containing catalog and namespace info

    Returns:
        MaterializeResult with metadata about free_users, paid_users, conversion_rate
    """
    session = spark
    catalog = streaming_config.catalog
    namespace = streaming_config.namespace

    auth_table = f"{catalog}.{namespace}.silver_auth_events"
    listen_table = f"{catalog}.{namespace}.silver_listen_events"
    target_table = f"{catalog}.{namespace}.gold_user_conversion_funnel"

    context.log.info(f"Reading from Silver tables: {auth_table}, {listen_table}")

    # Read from auth events table for level information
    df_auth = session.table(auth_table)

    # Calculate free users per event_date
    free_users_df = (
        df_auth.filter(col("level") == "free")
        .groupBy("event_date")
        .agg(countDistinct("userId").alias("free_users"))
        .select(col("event_date"), col("free_users"))
    )

    # Calculate paid users per event_date
    paid_users_df = (
        df_auth.filter(col("level") == "paid")
        .groupBy("event_date")
        .agg(countDistinct("userId").alias("paid_users"))
        .select(col("event_date"), col("paid_users"))
    )

    # Join free and paid users dataframes
    funnel_df = free_users_df.join(paid_users_df, "event_date", "outer").select(
        col("event_date"),
        col("free_users"),
        col("paid_users"),
    )

    # Fill null values with 0 for calculations
    funnel_df = funnel_df.fillna({"free_users": 0, "paid_users": 0})

    # Calculate conversion rate: (paid_users / free_users) * 100 (null-safe)
    result_df = funnel_df.withColumn(
        "conversion_rate",
        when(
            col("free_users") > 0, (col("paid_users") / col("free_users")) * 100
        ).otherwise(0.0),
    )

    output_rows = result_df.count()
    context.log.info(f"Computed conversion funnel for {output_rows} date records")

    # Get the event_date being processed (if partitioned, use partition key)
    event_date = context.partition_key if context.has_partition_key else "all_dates"
    context.log.info(f"Processing event_date: {event_date}")

    # Get metrics for metadata
    if output_rows > 0:
        metrics_row = result_df.select(
            "free_users", "paid_users", "conversion_rate"
        ).first()
        free_users = int(metrics_row["free_users"])
        paid_users = int(metrics_row["paid_users"])
        conversion_rate = float(metrics_row["conversion_rate"])
    else:
        free_users = 0
        paid_users = 0
        conversion_rate = 0.0

    context.log.info(
        f"Funnel metrics: free={free_users}, paid={paid_users}, "
        f"rate={conversion_rate:.2f}%"
    )

    # Write to Gold table with dynamic partition overwrite
    context.log.info(f"Writing to Gold table: {target_table}")

    write_mode = "overwrite"
    partition_overwrite_mode = "dynamic"

    result_df.write.mode(write_mode).option(
        "partitionOverwriteMode", partition_overwrite_mode
    ).partitionBy("event_date").format("iceberg").saveAsTable(target_table)

    context.log.info(f"✓ Successfully wrote {output_rows} rows to {target_table}")

    return dg.MaterializeResult(
        metadata={
            "event_date": dg.MetadataValue.text(event_date),
            "free_users": dg.MetadataValue.int(free_users),
            "paid_users": dg.MetadataValue.int(paid_users),
            "conversion_rate": dg.MetadataValue.float(conversion_rate),
            "source_tables": dg.MetadataValue.json(
                {"auth_events": auth_table, "listen_events": listen_table}
            ),
            "target_table": dg.MetadataValue.text(target_table),
            "output_rows": dg.MetadataValue.int(output_rows),
        }
    )
