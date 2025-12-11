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

import argparse
import os

import boto3
from pyspark.sql import SparkSession

# Dagster Pipes integration (optional)
try:
    from dagster_pipes import (
        PipesCliArgsParamsLoader,
        PipesS3ContextLoader,
        PipesS3MessageWriter,
        open_dagster_pipes,
    )

    PIPES_AVAILABLE = True
except ImportError:
    PIPES_AVAILABLE = False
    print("[WARN] dagster_pipes not installed, running without Pipes integration")

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
    parser = argparse.ArgumentParser(description="Compact Iceberg tables")
    parser.add_argument(
        "--polaris-uri",
        default="http://polaris:8181/api/catalog",
        help="Polaris catalog URI",
    )
    parser.add_argument(
        "--polaris-credential",
        required=True,
        help="Polaris credential in format client_id:client_secret",
    )
    parser.add_argument(
        "--catalog",
        default="lakehouse",
        help="Iceberg catalog name",
    )
    parser.add_argument(
        "--namespace",
        default="streamify",
        help="Iceberg namespace (database)",
    )
    parser.add_argument(
        "--table",
        help="Specific table to compact (optional, compacts all if not specified)",
    )
    # Dagster Pipes args
    parser.add_argument("--dagster-pipes-context", help="Pipes context (auto)")
    parser.add_argument("--dagster-pipes-messages", help="Pipes messages (auto)")

    return parser.parse_args()


def create_spark_session(args) -> SparkSession:
    """Create SparkSession configured for Iceberg and Polaris."""
    return (
        SparkSession.builder.appName("CompactTables")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{args.catalog}", "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(f"spark.sql.catalog.{args.catalog}.type", "rest")
        .config(f"spark.sql.catalog.{args.catalog}.uri", args.polaris_uri)
        .config(f"spark.sql.catalog.{args.catalog}.warehouse", args.catalog)
        .config(f"spark.sql.catalog.{args.catalog}.credential", args.polaris_credential)
        .config(f"spark.sql.catalog.{args.catalog}.scope", "PRINCIPAL_ROLE:ALL")
        .config(
            f"spark.sql.catalog.{args.catalog}.header.X-Iceberg-Access-Delegation",
            "vended-credentials",
        )
        .config(f"spark.sql.catalog.{args.catalog}.token-refresh-enabled", "true")
        .config("spark.sql.defaultCatalog", args.catalog)
        .getOrCreate()
    )


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
    spark = create_spark_session(args)
    spark.sparkContext.setLogLevel("WARN")

    # Run with or without Pipes
    if PIPES_AVAILABLE and args.dagster_pipes_context:
        # Running under Dagster Pipes
        s3_client = boto3.client(
            "s3",
            endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )

        with open_dagster_pipes(
            message_writer=PipesS3MessageWriter(client=s3_client),
            context_loader=PipesS3ContextLoader(client=s3_client),
            params_loader=PipesCliArgsParamsLoader(),
        ) as pipes:
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
    else:
        # Running standalone
        result = compact_all_tables(spark, args)
        print(f"\nResult: {result}")

    spark.stop()
    print("\n[DONE] Compaction complete")


if __name__ == "__main__":
    main()
