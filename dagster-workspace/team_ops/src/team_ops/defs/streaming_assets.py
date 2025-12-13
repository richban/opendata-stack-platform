"""Dagster assets for managing Spark Structured Streaming jobs via Spark Connect.

This module uses Spark Connect to launch long-running streaming jobs directly from Dagster.
No subprocess, no Pipes, no jar transfer - just clean remote Spark execution.
"""

import time

import dagster as dg
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json

from team_ops.defs.resources import SparkConnectResource, StreamingJobConfig
from team_ops.schemas import SCHEMAS as TOPIC_SCHEMAS


def create_namespace_if_not_exists(
    spark: SparkSession, catalog: str, namespace: str
) -> None:
    """Create Iceberg namespace if it doesn't exist."""
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")
    except Exception as e:
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
    config: StreamingJobConfig,
    topic: str,
    schema,
    context: dg.AssetExecutionContext,
):
    """Process a single Kafka stream and write to Iceberg."""
    table_name = f"{config.catalog}.{config.namespace}.bronze_{topic}"
    checkpoint_location = f"{config.checkpoint_path}/{topic}"

    # Read from Kafka
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON and add metadata
    parsed_df = (
        df.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .select("data.*", "kafka_partition", "kafka_offset", "kafka_timestamp")
        .withColumn("ingestion_time", current_timestamp())
    )

    # Write to Iceberg
    query = (
        parsed_df.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .option("fanout-enabled", "true")
        .toTable(table_name)
    )

    context.log.info(f"✓ Started stream for {topic} -> {table_name}")
    return query


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
        query = process_stream(session, streaming_config, topic, schema, context)
        queries.append(query)

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
