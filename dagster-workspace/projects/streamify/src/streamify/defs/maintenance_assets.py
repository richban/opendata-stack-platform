"""Dagster maintenance assets for Iceberg table operations.

This module contains scheduled maintenance assets that perform routine
operations on Iceberg tables like compaction to optimize query performance.
"""

import dagster as dg
from dagster import AssetKey

from typing import Any

from pyspark.sql import SparkSession

from streamify.defs.resources import StreamingJobConfig


@dg.asset(
    group_name="streamify",
    kinds={"spark", "iceberg"},
    owners=["team:team-ops"],
    deps=[AssetKey("bronze_streaming_job")],
    description="Compact small Parquet files in Bronze Iceberg tables via rewrite_data_files",
)
def bronze_compaction(
    context: dg.AssetExecutionContext,
    spark: dg.ResourceParam[SparkSession],
    streaming_config: StreamingJobConfig,
) -> dg.MaterializeResult:
    """Run Iceberg rewrite_data_files on all three Bronze tables.

    This compaction job consolidates small Parquet files from the 30-second
    micro-batches into larger, more efficient files to improve read performance.

    The rewrite_data_files procedure is called for each Bronze topic table:
    - bronze_listen_events
    - bronze_page_view_events
    - bronze_auth_events

    Args:
        context: Dagster asset execution context for logging
        spark: SparkConnectResource for Iceberg operations
        streaming_config: Configuration containing catalog and namespace info

    Returns:
        MaterializeResult with metadata about files rewritten per table
    """
    context.log.info("Starting Bronze Iceberg table compaction")

    session = spark
    catalog = streaming_config.catalog
    namespace = streaming_config.namespace
    topics = ["listen_events", "page_view_events", "auth_events"]

    compaction_results: dict[str, dict[str, Any]] = {}

    for topic in topics:
        table_name = f"{namespace}.bronze_{topic}"
        full_table_name = f"{catalog}.{table_name}"

        context.log.info(f"Compacting table: {full_table_name}")

        # Call Iceberg's rewrite_data_files procedure
        try:
            result = session.sql(
                f"CALL {catalog}.system.rewrite_data_files(table => '{table_name}')"
            )

            # Collect results - the procedure returns a DataFrame with compaction stats
            result_row = result.collect()[0] if result.count() > 0 else None

            if result_row:
                files_rewritten = getattr(result_row, "rewritten_data_files_count", 0)
                bytes_written = getattr(result_row, "written_data_files_size_in_bytes", 0)
            else:
                files_rewritten = 0
                bytes_written = 0

            compaction_results[topic] = {
                "files_rewritten": files_rewritten,
                "bytes_written": bytes_written,
            }

            context.log.info(
                f"✓ {table_name}: {files_rewritten} files rewritten, "
                f"{bytes_written} bytes written"
            )

        except Exception as e:
            context.log.error(f"Failed to compact {table_name}: {e}")
            compaction_results[topic] = {
                "files_rewritten": 0,
                "bytes_written": 0,
                "error": str(e),
            }

    context.log.info(f"Compaction complete for all {len(topics)} Bronze tables")

    return dg.MaterializeResult(
        metadata={
            "tables_compacted": dg.MetadataValue.json(compaction_results),
            "catalog": dg.MetadataValue.text(catalog),
            "namespace": dg.MetadataValue.text(namespace),
        }
    )
