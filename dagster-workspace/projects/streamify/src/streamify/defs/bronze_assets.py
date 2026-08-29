"""Dagster assets for managing Spark Structured Streaming jobs via Spark Connect.

This module uses Spark Connect to launch long-running streaming jobs directly
from Dagster. No subprocess, no Pipes, no jar transfer - just clean remote
Spark execution.

Streaming Configuration:
- Write interval: 30 seconds (processingTime trigger)
- Output mode: append
- Fanout enabled: true (for better parallelism)
- Checkpoint location: DuckDB-based for fault tolerance
"""

import traceback

import dagster as dg

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    from_json,
    sha2,
    to_date,
)
from pyspark.sql.types import StructType

from streamify.defs.resources import StreamingJobConfig
from streamify.schemas import META_SCHEMA, SCHEMAS as TOPIC_SCHEMAS


def process_stream(
    spark: SparkSession,
    streaming_config: StreamingJobConfig,
    topic: str,
    schema: StructType,
):
    """Transform Kafka stream: parse JSON, add event_id, extract event_date."""

    # Map host address (localhost:9093) to container address (kafka:9092)
    spark_kafka_servers = streaming_config.get_kafka_bootstrap_servers()

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", spark_kafka_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 100000)
        .load()
    )

    # Parse JSON, flatten struct, and add metadata - following Spark docs pattern
    parsed_df = (
        kafka_df.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("partition").alias("_kafka_partition"),
            col("offset").alias("_kafka_offset"),
            col("timestamp").alias("_kafka_timestamp"),
        )
        .select(
            "data.*",
            "_kafka_partition",
            "_kafka_offset",
            "_kafka_timestamp",
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
        .withColumn("event_ts", (col("ts") / 1000).cast("timestamp"))
        .withColumn("event_date", to_date(col("event_ts")))
        .withColumn("_processing_time", current_timestamp())
    )

    metadata_cols = [field.name for field in META_SCHEMA]
    data_cols = [field.name for field in schema.fields]

    return parsed_df.select(*data_cols, *metadata_cols)


def write_stream(
    df_stream,
    streaming_config: StreamingJobConfig,
    topic: str,
    context: dg.AssetExecutionContext,
):
    """Write streaming DataFrame to Iceberg table with 30-second micro-batches.

    Uses processingTime trigger to write every 30 seconds, avoiding small file problems.
    """
    table_name = f"bronze_{topic}"
    checkpoint_location = f"{streaming_config.checkpoint_path}/{topic}"

    df_out = (
        df_stream.writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", checkpoint_location)
        .option("fanout-enabled", "true")
        .queryName(f"bronze_{topic}")
        .toTable(table_name)
    )

    context.log.info(f"✓ Started stream for {topic} -> {table_name}")
    context.log.info("  Write interval: 30 seconds (processingTime trigger)")
    context.log.info(f"  Checkpoint: {checkpoint_location}")

    return df_out


@dg.asset(
    group_name="streamify",
    kinds={"spark", "iceberg", "kafka"},
    owners=["team:team-ops"],
    tags={"layer": "bronze", "schedule": "streaming"},
    description="Kafka to Iceberg Bronze streaming job (via Spark Connect)",
)
def bronze_streaming_job(
    context: dg.AssetExecutionContext,
    spark: dg.ResourceParam[SparkSession],
    streaming_config: StreamingJobConfig,
):
    """Launch Spark Structured Streaming job to ingest Kafka events to Iceberg Bronze.

    This asset uses Spark Connect to:
    1. Connect to the remote Spark cluster (no jar transfer needed)
    2. Start streaming queries for each Kafka topic
    3. Monitor the streams

    The streaming job runs continuously. This is a long-running asset.

    Note: In production, consider:
    - Running with a timeout or max duration
    - Using a separate scheduler/cron to restart if needed
    - Monitoring with sensors for health checks
    - Use `startingOffsets=earliest` for backfill, `latest` for continuous
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
        session = spark
        context.log.info("✓ Connected to Spark Connect")
    except Exception as e:
        context.log.error(f"Failed to connect to Spark: {e}")
        context.log.error(traceback.format_exc())
        raise

    # Start streams for each topic
    queries = []
    for topic, schema in TOPIC_SCHEMAS.items():
        context.log.info(f"Starting stream for {topic}...")
        df_stream = process_stream(session, streaming_config, topic, schema)
        df_out = write_stream(df_stream, streaming_config, topic, context)
        queries.append(df_out)

    context.log.info(f"All {len(queries)} streams running. Monitoring...")

    # Report materialization with rich metadata
    topics_started = list(TOPIC_SCHEMAS.keys())
    spark_ui_url = "http://localhost:8080"

    yield dg.MaterializeResult(
        metadata={
            "topics_started": dg.MetadataValue.json(topics_started),
            "spark_ui_url": dg.MetadataValue.url(spark_ui_url),
            "catalog": dg.MetadataValue.text(streaming_config.catalog),
            "checkpoint_base": dg.MetadataValue.text(streaming_config.checkpoint_path),
            "topics": dg.MetadataValue.json(topics_started),
            "namespace": streaming_config.namespace,
            "kafka_servers": streaming_config.kafka_bootstrap_servers,
            "num_streams": len(queries),
        }
    )
    # Server-side streaming continues on the Spark Connect JVM after we exit.
