#!/usr/bin/env python3
"""Expire old Iceberg snapshots.

This maintenance job:
1. Runs Iceberg's expire_snapshots procedure on all tables
2. Removes snapshots older than 7 days
3. Retains at least 10 snapshots for time travel

Should run daily.

Integrates with Dagster Pipes for observability.

Usage:
    spark-submit \\
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1,\\
org.apache.iceberg:iceberg-aws-bundle:1.7.1 \\
        expire_snapshots.py \\
        --polaris-uri http://polaris:8181/api/catalog \\
        --polaris-credential <client_id>:<client_secret>
"""

import argparse
import os
from datetime import datetime, timedelta, timezone

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

# Tables to expire snapshots
TABLES = [
    "bronze_listen_events",
    "bronze_page_view_events",
    "bronze_auth_events",
    "silver_listen_events",
    "silver_page_view_events",
    "silver_auth_events",
    "silver_user_sessions",
]

# Retention settings
RETENTION_DAYS = 7
RETAIN_LAST_SNAPSHOTS = 10


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Expire old Iceberg snapshots")
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
        "--retention-days",
        type=int,
        default=RETENTION_DAYS,
        help=f"Days to retain snapshots (default: {RETENTION_DAYS})",
    )
    parser.add_argument(
        "--table",
        help="Specific table to expire (optional, expires all if not specified)",
    )
    # Dagster Pipes args
    parser.add_argument("--dagster-pipes-context", help="Pipes context (auto)")
    parser.add_argument("--dagster-pipes-messages", help="Pipes messages (auto)")

    return parser.parse_args()


def create_spark_session(args) -> SparkSession:
    """Create SparkSession configured for Iceberg and Polaris."""
    return (
        SparkSession.builder.appName("ExpireSnapshots")
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


def expire_snapshots_for_table(
    spark: SparkSession, catalog: str, namespace: str, table: str, retention_days: int
) -> dict:
    """Expire snapshots for a single table."""
    full_table_name = f"{catalog}.{namespace}.{table}"

    print(f"Expiring snapshots for {full_table_name}...")

    # Check if table exists
    try:
        spark.table(full_table_name)
    except Exception:
        print(f"  Table {full_table_name} does not exist, skipping")
        return {"table": table, "status": "skipped", "reason": "not_found"}

    # Calculate cutoff timestamp
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

    try:
        # Run expire_snapshots procedure
        result = spark.sql(f"""
            CALL {catalog}.system.expire_snapshots(
                table => '{namespace}.{table}',
                older_than => TIMESTAMP '{cutoff_str}',
                retain_last => {RETAIN_LAST_SNAPSHOTS}
            )
        """)

        # Get results
        row = result.collect()[0]
        deleted_files = row["deleted_data_files_count"]
        deleted_manifests = row["deleted_manifest_files_count"]
        deleted_manifest_lists = row["deleted_manifest_lists_count"]

        print(
            f"  Deleted: {deleted_files} data files, "
            f"{deleted_manifests} manifests, {deleted_manifest_lists} lists"
        )

        return {
            "table": table,
            "status": "success",
            "deleted_data_files": deleted_files,
            "deleted_manifest_files": deleted_manifests,
            "deleted_manifest_lists": deleted_manifest_lists,
        }

    except Exception as e:
        error_msg = str(e)
        if (
            "nothing to expire" in error_msg.lower()
            or "no snapshots" in error_msg.lower()
        ):
            print(f"  Nothing to expire")
            return {"table": table, "status": "nothing_to_expire"}
        else:
            print(f"  Error: {error_msg[:200]}")
            return {"table": table, "status": "error", "error": error_msg[:500]}


def expire_all_tables(spark: SparkSession, args) -> dict:
    """Expire snapshots for all tables."""
    tables_to_expire = [args.table] if args.table else TABLES

    results = []
    total_deleted_files = 0
    total_deleted_manifests = 0
    errors = 0

    for table in tables_to_expire:
        result = expire_snapshots_for_table(
            spark, args.catalog, args.namespace, table, args.retention_days
        )
        results.append(result)

        if result["status"] == "success":
            total_deleted_files += result.get("deleted_data_files", 0)
            total_deleted_manifests += result.get("deleted_manifest_files", 0)
        elif result["status"] == "error":
            errors += 1

    return {
        "tables_processed": len(tables_to_expire),
        "retention_days": args.retention_days,
        "total_deleted_data_files": total_deleted_files,
        "total_deleted_manifest_files": total_deleted_manifests,
        "errors": errors,
        "details": results,
    }


def main():
    """Main entry point."""
    args = parse_args()

    print("=" * 70)
    print(f"EXPIRE SNAPSHOTS (retention: {args.retention_days} days)")
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
            pipes.log.info(
                f"Starting snapshot expiry (retention: {args.retention_days} days)"
            )

            result = expire_all_tables(spark, args)

            pipes.report_asset_materialization(
                metadata={
                    "tables_processed": {
                        "raw_value": result["tables_processed"],
                        "type": "int",
                    },
                    "retention_days": {
                        "raw_value": result["retention_days"],
                        "type": "int",
                    },
                    "total_deleted_data_files": {
                        "raw_value": result["total_deleted_data_files"],
                        "type": "int",
                    },
                    "total_deleted_manifest_files": {
                        "raw_value": result["total_deleted_manifest_files"],
                        "type": "int",
                    },
                    "errors": {"raw_value": result["errors"], "type": "int"},
                },
            )

            pipes.log.info(
                f"Snapshot expiry complete: {result['tables_processed']} tables"
            )
    else:
        # Running standalone
        result = expire_all_tables(spark, args)
        print(f"\nResult: {result}")

    spark.stop()
    print("\n[DONE] Snapshot expiry complete")


if __name__ == "__main__":
    main()
