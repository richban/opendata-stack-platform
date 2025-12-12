#!/usr/bin/env python3
"""Deduplicate Bronze events and write to Silver tables.

This batch job:
1. Reads from Bronze Iceberg tables
2. Removes duplicates using event_id (keeps first occurrence)
3. Writes deduplicated data to Silver tables

Integrates with Dagster Pipes for observability.

Usage:
    spark-submit \\
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1,\\
org.apache.iceberg:iceberg-aws-bundle:1.7.1 \\
        deduplicate_events.py \\
        --polaris-uri http://polaris:8181/api/catalog \\
        --polaris-credential <client_id>:<client_secret> \\
        --event-date 2025-01-15 \\
        --topic listen_events
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
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


def parse_args():
    """Parse command line arguments."""
    parser = get_common_arg_parser("Deduplicate Bronze events to Silver tables")
    parser.add_argument(
        "--event-date",
        required=True,
        help="Date to process (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--topic",
        required=True,
        choices=["listen_events", "page_view_events", "auth_events"],
        help="Topic/table to deduplicate",
    )
    return parser.parse_args()


def create_silver_table_if_not_exists(spark: SparkSession, args):
    """Create Silver table if it doesn't exist (same schema as Bronze)."""
    bronze_table = f"{args.catalog}.{args.namespace}.bronze_{args.topic}"
    silver_table = f"{args.catalog}.{args.namespace}.silver_{args.topic}"

    # Get Bronze table schema
    bronze_df = spark.table(bronze_table).limit(0)

    # Check if Silver table exists
    try:
        spark.table(silver_table)
        print(f"Silver table {silver_table} already exists")
    except Exception:
        # Create Silver table with same schema
        columns = []
        for field in bronze_df.schema.fields:
            columns.append(f"{field.name} {field.dataType.simpleString()}")

        columns_str = ", ".join(columns)

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {silver_table} (
                {columns_str}
            )
            USING iceberg
            PARTITIONED BY (event_date)
        """
        spark.sql(create_sql)
        print(f"Created Silver table {silver_table}")


def deduplicate(spark: SparkSession, args) -> dict:
    """Deduplicate events for a specific date."""
    bronze_table = f"{args.catalog}.{args.namespace}.bronze_{args.topic}"
    silver_table = f"{args.catalog}.{args.namespace}.silver_{args.topic}"

    print(f"Deduplicating {bronze_table} for date {args.event_date}")

    # Read Bronze data for the specific date
    bronze_df = spark.table(bronze_table).filter(col("event_date") == args.event_date)

    input_count = bronze_df.count()
    print(f"Input records: {input_count}")

    if input_count == 0:
        print("No records to process")
        return {
            "input_count": 0,
            "output_count": 0,
            "duplicates_removed": 0,
        }

    # Deduplicate using window function
    # Keep the first occurrence (earliest _processing_time)
    window_spec = Window.partitionBy("event_id").orderBy(col("_processing_time").asc())

    deduped_df = (
        bronze_df.withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    output_count = deduped_df.count()
    duplicates_removed = input_count - output_count

    print(f"Output records: {output_count}")
    print(f"Duplicates removed: {duplicates_removed}")

    # Write to Silver table using MERGE to handle re-runs
    # First, delete existing data for this date
    spark.sql(f"""
        DELETE FROM {silver_table}
        WHERE event_date = '{args.event_date}'
    """)

    # Then append deduplicated data
    deduped_df.writeTo(silver_table).append()

    print(f"Wrote {output_count} records to {silver_table}")

    return {
        "input_count": input_count,
        "output_count": output_count,
        "duplicates_removed": duplicates_removed,
    }


def main():
    """Main entry point."""
    args = parse_args()

    print("=" * 70)
    print(f"DEDUPLICATE: {args.topic} for {args.event_date}")
    print("=" * 70)

    # Create Spark session
    app_name = f"Deduplicate-{args.topic}-{args.event_date}"
    spark = create_spark_session(app_name, args)
    spark.sparkContext.setLogLevel("WARN")

    # Ensure Silver table exists
    create_silver_table_if_not_exists(spark, args)

    with open_pipes(args) as pipes:
        pipes.log.info(f"Starting deduplication for {args.topic}")

        result = deduplicate(spark, args)

        pipes.report_asset_materialization(
            metadata={
                "topic": args.topic,
                "event_date": args.event_date,
                "input_count": {"raw_value": result["input_count"], "type": "int"},
                "output_count": {"raw_value": result["output_count"], "type": "int"},
                "duplicates_removed": {
                    "raw_value": result["duplicates_removed"],
                    "type": "int",
                },
            },
        )

        pipes.log.info(f"Deduplication complete: {result}")
        if not args.dagster_pipes_context:
            print(f"\nResult: {result}")

    spark.stop()
    print("\n[DONE] Deduplication complete")


if __name__ == "__main__":
    main()
