"""Dagster assets for managing Spark Structured Streaming jobs via Spark Connect.

This module uses Spark Connect to launch long-running streaming jobs directly from Dagster.
No subprocess, no Pipes, no jar transfer - just clean remote Spark execution.

Streaming Configuration:
- Write interval: 30 seconds (processingTime trigger)
- Output mode: append
- Fanout enabled: true (for better parallelism)
- Checkpoint location: DuckDB-based for fault tolerance
"""

import dagster as dg

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    sha2,
    concat_ws,
    to_date,
    from_unixtime,
)
from pyspark.sql.types import StructType

from team_ops.defs.resources import SparkConnectResource, StreamingJobConfig
from team_ops.schemas import SCHEMAS as TOPIC_SCHEMAS


def create_namespace_if_not_exists(
    spark: SparkSession, catalog: str, namespace: str
) -> None:
    """Create Iceberg namespace if it doesn't exist."""
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")
    except Exception:
        # Namespace might already exist, that's fine
        pass


def create_table_if_not_exists(
    spark: SparkSession,
    catalog: str,
    namespace: str,
    topic: str,
    schema,
) -> None:
    """Create Iceberg table if it doesn't exist."""
    table_name = f"{catalog}.{namespace}.bronze_{topic}"

    # Check if table exists
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        return  # Table exists
    except Exception:
        pass  # Table doesn't exist, create it

    # Build schema DDL from the provided schema
    fields = []
    for field in schema.fields:
        field_type = field.dataType.simpleString()
        # Iceberg/Spark SQL doesn't use NULL keyword, only NOT NULL for non-nullable fields
        nullable = "" if field.nullable else "NOT NULL"
        fields.append(f"{field.name} {field_type} {nullable}".strip())

    # Add metadata columns
    fields.extend(
        [
            "kafka_partition INT",
            "kafka_offset BIGINT",
            "kafka_timestamp TIMESTAMP",
            "ingestion_time TIMESTAMP NOT NULL",
        ]
    )

    schema_ddl = ",\n    ".join(fields)

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {schema_ddl}
    )
    USING iceberg
    PARTITIONED BY (days(ingestion_time))
    """

    spark.sql(create_sql)


def process_stream(
    spark: SparkSession,
    streaming_config: StreamingJobConfig,
    topic: str,
    schema: StructType,
):
    """Transform Kafka stream: parse JSON, add event_id, extract event_date."""

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", streaming_config.kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        kafka_df.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("partition").alias("_kafka_partition"),
            col("offset").alias("_kafka_offset"),
        )
        .select(
            "data.*",
            "_kafka_partition",
            "_kafka_offset",
        )
        .withColumn(
            "event_id",
            sha2(
                concat_ws(
                    "_",
                    col("userId").cast("string"),
                    col("sessionId").cast("string"),
                    col("ts").cast("string"),
                ),
                256,
            ),
        )
        .withColumn("event_date", to_date(from_unixtime(col("ts") / 1000)))
        .withColumn("_processing_time", current_timestamp())
    )

    return parsed_df


def write_stream(
    df_stream,
    streaming_config: StreamingJobConfig,
    topic: str,
    context: dg.AssetExecutionContext,
):
    """Write streaming DataFrame to Iceberg table with 30-second micro-batches.

    Uses processingTime trigger to write every 30 seconds, avoiding small file problems.
    """
    table_name = f"{streaming_config.catalog}.{streaming_config.namespace}.bronze_{topic}"
    checkpoint_location = f"{streaming_config.checkpoint_path}/{topic}"

    df_out = (
        df_stream.writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", checkpoint_location)
        .option("fanout-enabled", "true")
        .toTable(table_name)
    )

    context.log.info(f"✓ Started stream for {topic} -> {table_name}")
    context.log.info(f"  Write interval: 30 seconds (processingTime trigger)")
    context.log.info(f"  Checkpoint: {checkpoint_location}")

    return df_out


@dg.asset(
    group_name="streaming",
    compute_kind="spark-streaming",
    description="Kafka to Iceberg Bronze streaming job (via Spark Connect)",
)
def bronze_streaming_job(
    context: dg.AssetExecutionContext,
    spark: SparkConnectResource,
    streaming_config: StreamingJobConfig,
):
    """Launch Spark Structured Streaming job to ingest Kafka events to Iceberg Bronze.

    This asset uses Spark Connect to:
    1. Connect to the remote Spark cluster (no jar transfer needed)
    2. Create Iceberg namespace and tables
    3. Start streaming queries for each Kafka topic
    4. Monitor the streams

    The streaming job runs continuously. This is a long-running asset.

    Note: In production, consider:
    - Running with a timeout or max duration
    - Using a separate scheduler/cron to restart if needed
    - Monitoring with sensors for health checks
    """
    try:
        context.log.info("STREAMIFY - Kafka to Iceberg Streaming (Spark Connect)")
        context.log.info(f"Kafka:       {streaming_config.kafka_bootstrap_servers}")
        context.log.info(
            f"Catalog:     {streaming_config.catalog}.{streaming_config.namespace}"
        )
        context.log.info(f"Checkpoints: {streaming_config.checkpoint_path}")

        # Get Spark session via Connect
        context.log.info("Connecting to Spark Connect...")
        session = spark.get_session()
        context.log.info("✓ Connected to Spark Connect")
    except Exception as e:
        context.log.error(f"Failed to connect to Spark: {e}")
        import traceback

        context.log.error(traceback.format_exc())
        raise

    # Create namespace
    context.log.info(
        f"Creating namespace {streaming_config.catalog}.{streaming_config.namespace}..."
    )
    create_namespace_if_not_exists(
        session, streaming_config.catalog, streaming_config.namespace
    )
    context.log.info("✓ Namespace ready")

    # Start streams for each topic
    queries = []
    for topic, schema in TOPIC_SCHEMAS.items():
        context.log.info(f"Creating table for {topic}...")
        create_table_if_not_exists(
            session, streaming_config.catalog, streaming_config.namespace, topic, schema
        )

        context.log.info(f"Starting stream for {topic}...")
        df_stream = process_stream(session, streaming_config, topic, schema)
        df_out = write_stream(df_stream, streaming_config, topic, context)
        queries.append(df_out)

    context.log.info(f"All {len(queries)} streams running. Monitoring...")

    # Report materialization
    yield dg.MaterializeResult(
        metadata={
            "topics": dg.MetadataValue.json(list(TOPIC_SCHEMAS.keys())),
            "catalog": streaming_config.catalog,
            "namespace": streaming_config.namespace,
            "kafka_servers": streaming_config.kafka_bootstrap_servers,
            "num_streams": len(queries),
        }
    )

    # Keep streams alive (this will run indefinitely)
    context.log.info("Streams are running. Waiting for termination...")
    session.streams.awaitAnyTermination()
