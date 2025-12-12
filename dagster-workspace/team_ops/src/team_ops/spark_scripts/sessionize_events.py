#!/usr/bin/env python3
"""Sessionize user events from Silver tables.

This batch job:
1. Reads deduplicated events from Silver tables
2. Groups events by userId and sessionId
3. Calculates session metrics (duration, counts, level changes)
4. Detects sessions based on 30-minute inactivity timeout
5. Writes to silver_user_sessions table

Key metrics for churn analysis:
- session_duration_sec: How long users engage
- level_at_start / level_at_end: Subscription changes (premium -> free = churn signal)
- listen_count: Engagement depth
- pages_visited: User journey

Integrates with Dagster Pipes for observability.

Usage:
    spark-submit \\
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1,\\
org.apache.iceberg:iceberg-aws-bundle:1.7.1 \\
        sessionize_events.py \\
        --polaris-uri http://polaris:8181/api/catalog \\
        --polaris-credential <client_id>:<client_secret> \\
        --event-date 2025-01-15
"""

try:
    from team_ops.spark_scripts.common import (
        create_spark_session,
        get_common_arg_parser,
        open_pipes,
    )
except ImportError:
    from common import create_spark_session, get_common_arg_parser, open_pipes

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    collect_set,
    count,
    current_timestamp,
    first,
    from_unixtime,
    last,
    lit,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    when,
)

# Session timeout in seconds (30 minutes)
SESSION_TIMEOUT_SEC = 30 * 60


def parse_args():
    """Parse command line arguments."""
    parser = get_common_arg_parser("Sessionize user events from Silver tables")
    parser.add_argument(
        "--event-date",
        required=True,
        help="Date to process (YYYY-MM-DD)",
    )
    return parser.parse_args()


def create_sessions_table_if_not_exists(spark: SparkSession, args):
    """Create the user_sessions table if it doesn't exist."""
    table_name = f"{args.catalog}.{args.namespace}.silver_user_sessions"

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            user_id BIGINT,
            session_id INT,
            session_date DATE,
            session_start_ts TIMESTAMP,
            session_end_ts TIMESTAMP,
            session_duration_sec BIGINT,

            -- Engagement metrics
            listen_count INT,
            page_view_count INT,
            auth_event_count INT,
            total_listen_duration_sec DOUBLE,

            -- Content (as arrays)
            songs_played ARRAY<STRING>,
            artists_played ARRAY<STRING>,
            pages_visited ARRAY<STRING>,

            -- User state (for churn analysis)
            level_at_start STRING,
            level_at_end STRING,
            level_changed BOOLEAN,

            -- Geography (from first event)
            city STRING,
            state STRING,

            -- Metadata
            _processed_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (session_date)
    """

    spark.sql(create_sql)
    print(f"Sessions table {table_name} ready")


def sessionize(spark: SparkSession, args) -> dict:
    """Build user sessions from Silver events."""
    catalog = args.catalog
    namespace = args.namespace
    event_date = args.event_date

    sessions_table = f"{catalog}.{namespace}.silver_user_sessions"

    print(f"Building sessions for {event_date}")

    # Read Silver tables for the date
    listen_df = (
        spark.table(f"{catalog}.{namespace}.silver_listen_events")
        .filter(col("event_date") == event_date)
        .select(
            col("userId").alias("user_id"),
            col("sessionId").alias("session_id"),
            col("ts"),
            col("level"),
            col("city"),
            col("state"),
            col("song"),
            col("artist"),
            col("duration").alias("listen_duration"),
        )
        .withColumn("event_type", lit("listen"))
    )

    page_view_df = (
        spark.table(f"{catalog}.{namespace}.silver_page_view_events")
        .filter(col("event_date") == event_date)
        .select(
            col("userId").alias("user_id"),
            col("sessionId").alias("session_id"),
            col("ts"),
            col("level"),
            col("city"),
            col("state"),
            col("page"),
        )
        .withColumn("event_type", lit("page_view"))
        .withColumn("song", lit(None))
        .withColumn("artist", lit(None))
        .withColumn("listen_duration", lit(None))
    )

    auth_df = (
        spark.table(f"{catalog}.{namespace}.silver_auth_events")
        .filter(col("event_date") == event_date)
        .select(
            col("userId").alias("user_id"),
            col("sessionId").alias("session_id"),
            col("ts"),
            col("level"),
            col("city"),
            col("state"),
        )
        .withColumn("event_type", lit("auth"))
        .withColumn("page", lit(None))
        .withColumn("song", lit(None))
        .withColumn("artist", lit(None))
        .withColumn("listen_duration", lit(None))
    )

    # Combine all events
    all_events = listen_df.unionByName(
        page_view_df, allowMissingColumns=True
    ).unionByName(auth_df, allowMissingColumns=True)

    total_events = all_events.count()
    print(f"Total events: {total_events}")

    if total_events == 0:
        print("No events to process")
        return {
            "total_events": 0,
            "sessions_created": 0,
        }

    # Aggregate by user_id, session_id
    sessions_df = (
        all_events.groupBy("user_id", "session_id")
        .agg(
            # Timestamps
            spark_min("ts").alias("session_start_ts_ms"),
            spark_max("ts").alias("session_end_ts_ms"),
            # Counts by event type
            count(when(col("event_type") == "listen", 1)).alias("listen_count"),
            count(when(col("event_type") == "page_view", 1)).alias("page_view_count"),
            count(when(col("event_type") == "auth", 1)).alias("auth_event_count"),
            # Listen duration
            spark_sum("listen_duration").alias("total_listen_duration_sec"),
            # Content arrays
            collect_set("song").alias("songs_played"),
            collect_set("artist").alias("artists_played"),
            collect_set("page").alias("pages_visited"),
            # Level at start/end (ordered by ts)
            first("level", ignorenulls=True).alias("level_at_start"),
            last("level", ignorenulls=True).alias("level_at_end"),
            # Geography (from first event)
            first("city", ignorenulls=True).alias("city"),
            first("state", ignorenulls=True).alias("state"),
        )
        # Calculate derived fields
        .withColumn(
            "session_start_ts",
            from_unixtime(col("session_start_ts_ms") / 1000).cast("timestamp"),
        )
        .withColumn(
            "session_end_ts",
            from_unixtime(col("session_end_ts_ms") / 1000).cast("timestamp"),
        )
        .withColumn(
            "session_duration_sec",
            ((col("session_end_ts_ms") - col("session_start_ts_ms")) / 1000).cast("long"),
        )
        .withColumn("level_changed", col("level_at_start") != col("level_at_end"))
        .withColumn("session_date", lit(event_date).cast("date"))
        .withColumn("_processed_at", current_timestamp())
        # Select final columns
        .select(
            "user_id",
            "session_id",
            "session_date",
            "session_start_ts",
            "session_end_ts",
            "session_duration_sec",
            "listen_count",
            "page_view_count",
            "auth_event_count",
            "total_listen_duration_sec",
            "songs_played",
            "artists_played",
            "pages_visited",
            "level_at_start",
            "level_at_end",
            "level_changed",
            "city",
            "state",
            "_processed_at",
        )
    )

    sessions_count = sessions_df.count()
    print(f"Sessions created: {sessions_count}")

    # Delete existing sessions for this date (idempotent)
    spark.sql(f"""
        DELETE FROM {sessions_table}
        WHERE session_date = '{event_date}'
    """)

    # Write sessions
    sessions_df.writeTo(sessions_table).append()

    print(f"Wrote {sessions_count} sessions to {sessions_table}")

    # Calculate some stats for observability
    level_changes = sessions_df.filter(col("level_changed") == True).count()
    avg_duration = sessions_df.agg({"session_duration_sec": "avg"}).collect()[0][0] or 0

    return {
        "total_events": total_events,
        "sessions_created": sessions_count,
        "level_changes_detected": level_changes,
        "avg_session_duration_sec": round(avg_duration, 2),
    }


def main():
    """Main entry point."""
    args = parse_args()

    print("=" * 70)
    print(f"SESSIONIZE: Building user sessions for {args.event_date}")
    print("=" * 70)

    # Create Spark session
    app_name = f"Sessionize-{args.event_date}"
    spark = create_spark_session(app_name, args)
    spark.sparkContext.setLogLevel("WARN")

    # Ensure sessions table exists
    create_sessions_table_if_not_exists(spark, args)

    with open_pipes(args) as pipes:
        pipes.log.info(f"Starting sessionization for {args.event_date}")

        result = sessionize(spark, args)

        pipes.report_asset_materialization(
            metadata={
                "event_date": args.event_date,
                "total_events": {"raw_value": result["total_events"], "type": "int"},
                "sessions_created": {
                    "raw_value": result["sessions_created"],
                    "type": "int",
                },
                "level_changes_detected": {
                    "raw_value": result["level_changes_detected"],
                    "type": "int",
                },
                "avg_session_duration_sec": {
                    "raw_value": result["avg_session_duration_sec"],
                    "type": "float",
                },
            },
        )

        pipes.log.info(f"Sessionization complete: {result}")
        if not args.dagster_pipes_context:
            print(f"\nResult: {result}")

    spark.stop()
    print("\n[DONE] Sessionization complete")


if __name__ == "__main__":
    main()
