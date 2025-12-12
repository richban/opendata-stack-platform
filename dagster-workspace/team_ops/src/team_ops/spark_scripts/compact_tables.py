#!/usr/bin/env python3
"""Compact Iceberg tables to merge small files.

This maintenance job:
1. Runs Iceberg's rewrite_data_files procedure on all tables
2. Merges small files into larger ones (target: 128MB)
3. Improves query performance

Should run every 4 hours.

Integrates with Dagster Pipes for observability.

Usage:
    spark-submit \\
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1,\\
org.apache.iceberg:iceberg-aws-bundle:1.7.1 \\
        compact_tables.py \\
        --polaris-uri http://polaris:8181/api/catalog \\
        --polaris-credential <client_id>:<client_secret>
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

# Tables to compact
TABLES = [
    "bronze_listen_events",
    "bronze_page_view_events",
    "bronze_auth_events",
    "silver_listen_events",
    "silver_page_view_events",
    "silver_auth_events",
    "silver_user_sessions",
]

# Target file size (128 MB)
TARGET_FILE_SIZE_BYTES = 128 * 1024 * 1024


def parse_args():
    """Parse command line arguments."""
    parser = get_common_arg_parser("Compact Iceberg tables")
    parser.add_argument(
        "--table",
        help="Specific table to compact (optional, compacts all if not specified)",
    )
    return parser.parse_args()


def compact_table(spark: SparkSession, catalog: str, namespace: str, table: str) -> dict:
    """Run compaction on a single table."""
    full_table_name = f"{catalog}.{namespace}.{table}"

    print(f"Compacting {full_table_name}...")

    # Check if table exists
    try:
        spark.table(full_table_name)
    except Exception as e:
        print(f"  Table {full_table_name} does not exist, skipping")
        return {"table": table, "status": "skipped", "reason": "not_found"}

    try:
        # Run rewrite_data_files procedure
        result = spark.sql(f"""
            CALL {catalog}.system.rewrite_data_files(
                table => '{namespace}.{table}',
                options => map(
                    'target-file-size-bytes', '{TARGET_FILE_SIZE_BYTES}',
                    'min-input-files', '2'
                )
            )
        """)

        # Get results
        row = result.collect()[0]
        rewritten_files = row["rewritten_data_files_count"]
        added_files = row["added_data_files_count"]

        print(f"  Rewritten: {rewritten_files} files -> {added_files} files")

        return {
            "table": table,
            "status": "success",
            "rewritten_files": rewritten_files,
            "added_files": added_files,
        }

    except Exception as e:
        error_msg = str(e)
        # "Nothing to compact" is not an error
        if (
            "nothing to compact" in error_msg.lower()
            or "no data files" in error_msg.lower()
        ):
            print(f"  Nothing to compact")
            return {"table": table, "status": "nothing_to_compact"}
        else:
            print(f"  Error: {error_msg[:200]}")
            return {"table": table, "status": "error", "error": error_msg[:500]}


def compact_all_tables(spark: SparkSession, args) -> dict:
    """Compact all tables."""
    tables_to_compact = [args.table] if args.table else TABLES

    results = []
    total_rewritten = 0
    total_added = 0
    errors = 0

    for table in tables_to_compact:
        result = compact_table(spark, args.catalog, args.namespace, table)
        results.append(result)

        if result["status"] == "success":
            total_rewritten += result.get("rewritten_files", 0)
            total_added += result.get("added_files", 0)
        elif result["status"] == "error":
            errors += 1

    return {
        "tables_processed": len(tables_to_compact),
        "total_rewritten_files": total_rewritten,
        "total_added_files": total_added,
        "errors": errors,
        "details": results,
    }


def main():
    """Main entry point."""
    args = parse_args()

    print("=" * 70)
    print("COMPACT TABLES")
    print("=" * 70)

    # Create Spark session
    spark = create_spark_session("CompactTables", args)
    spark.sparkContext.setLogLevel("WARN")

    with open_pipes(args) as pipes:
        pipes.log.info("Starting table compaction")

        result = compact_all_tables(spark, args)

        pipes.report_asset_materialization(
            metadata={
                "tables_processed": {
                    "raw_value": result["tables_processed"],
                    "type": "int",
                },
                "total_rewritten_files": {
                    "raw_value": result["total_rewritten_files"],
                    "type": "int",
                },
                "total_added_files": {
                    "raw_value": result["total_added_files"],
                    "type": "int",
                },
                "errors": {"raw_value": result["errors"], "type": "int"},
            },
        )

        pipes.log.info(f"Compaction complete: {result['tables_processed']} tables")
        if not args.dagster_pipes_context:
             print(f"\nResult: {result}")

    spark.stop()
    print("\n[DONE] Compaction complete")


if __name__ == "__main__":
    main()
