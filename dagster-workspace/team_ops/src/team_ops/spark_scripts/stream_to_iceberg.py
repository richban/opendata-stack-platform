#!/usr/bin/env python3
"""Spark Structured Streaming: Kafka -> Iceberg Bronze Tables with Dagster Pipes.

This script runs continuously, consuming events from Kafka topics
and writing them to Iceberg tables in the lakehouse catalog.

This script is orchestrated via Dagster Pipes - Dagster launches it via spark-submit
and monitors its progress via S3-based message passing.

Configuration is passed via Dagster Pipes extras (no argparse needed!).

Events:
- listen_events: Song play events
- page_view_events: Page navigation events
- auth_events: Authentication events

Each event gets:
- event_id: SHA256 hash for deduplication (userId + sessionId + ts)
- event_date: Derived from ts for partitioning
- _processing_time: When the event was processed
- _kafka_partition: Source Kafka partition
- _kafka_offset: Source Kafka offset
"""

import time

from dagster_pipes import (
    PipesCliArgsParamsLoader,
    PipesS3ContextLoader,
    PipesS3MessageWriter,
    open_dagster_pipes,
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    from_json,
    from_unixtime,
    sha2,
    to_date,
)
from pyspark.sql.types import StructType

from team_ops.defs.resources import (
    SparkConnectResource,
    StreamingJobConfig,
    create_s3_resource,
)
from team_ops.schemas import SCHEMAS as TOPIC_SCHEMAS


def create_namespace_if_not_exists(spark: SparkSession, catalog: str, namespace: str):
    """Create the namespace if it doesn't exist."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")
    print(f"✓ Namespace {catalog}.{namespace} ready")


def create_table_if_not_exists(
    spark: SparkSession, catalog: str, namespace: str, topic: str, schema: StructType
):
    """Create Iceberg table if it doesn't exist."""
    table_name = f"{catalog}.{namespace}.bronze_{topic}"

    columns = []
    for field in schema.fields:
        spark_type = field.dataType.simpleString()
        columns.append(f"{field.name} {spark_type}")

    columns.extend(
        [
            "event_id STRING",
            "event_date DATE",
            "_processing_time TIMESTAMP",
            "_kafka_partition INT",
            "_kafka_offset BIGINT",
        ]
    )

    columns_str = ", ".join(columns)

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns_str}
        )
        USING iceberg
        PARTITIONED BY (event_date)
    """

    spark.sql(create_sql)
    print(f"✓ Table {table_name} ready")


def process_stream(
    spark: SparkSession,
    streaming_config: StreamingJobConfig,
    topic: str,
    schema: StructType,
    pipes,
):
    """Create and start a streaming query for a topic."""
    table_name = f"{streaming_config.catalog}.{streaming_config.namespace}.bronze_{topic}"
    checkpoint_location = f"{streaming_config.checkpoint_path}/{topic}"

    pipes.log.info(f"Starting stream: {topic} -> {table_name}")

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

    query = (
        parsed_df.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .option("fanout-enabled", "true")
        .toTable(table_name)
    )

    return query


def main():
    """Main entry point with Dagster Pipes integration.

    Configuration comes from StreamingJobConfig resource (instantiated from env vars).
    No need for Pipes extras - both orchestrator and external process read same env vars!
    Reuses SparkConnectResource and S3Resource to avoid duplication.
    """
    s3_resource = create_s3_resource()
    s3_client = s3_resource.get_client()

    with open_dagster_pipes(
        message_writer=PipesS3MessageWriter(client=s3_client),
        context_loader=PipesS3ContextLoader(client=s3_client),
        params_loader=PipesCliArgsParamsLoader(),
    ) as pipes:
        pipes.log.info("=" * 70)
        pipes.log.info("STREAMIFY - Kafka to Iceberg Streaming (Dagster Pipes)")
        pipes.log.info("=" * 70)

        streaming_config = StreamingJobConfig()

        pipes.log.info(f"Kafka:       {streaming_config.kafka_bootstrap_servers}")
        pipes.log.info(
            f"Catalog:     {streaming_config.catalog}.{streaming_config.namespace}"
        )
        pipes.log.info(f"Checkpoints: {streaming_config.checkpoint_path}")
        pipes.log.info("=" * 70)

        spark_resource = SparkConnectResource()
        spark = spark_resource.create_regular_session("StreamToIceberg")
        spark.sparkContext.setLogLevel("WARN")

        create_namespace_if_not_exists(
            spark, streaming_config.catalog, streaming_config.namespace
        )

        queries = []
        for topic, schema in TOPIC_SCHEMAS.items():
            create_table_if_not_exists(
                spark, streaming_config.catalog, streaming_config.namespace, topic, schema
            )
            query = process_stream(spark, streaming_config, topic, schema, pipes)
            queries.append(query)
            pipes.log.info(f"✓ Stream started for {topic}")

        pipes.log.info("\n" + "=" * 70)
        pipes.log.info("All streams running. Monitoring...")
        pipes.log.info("=" * 70 + "\n")

        pipes.report_asset_materialization(
            metadata={
                "topics": {"raw_value": list(TOPIC_SCHEMAS.keys()), "type": "json"},
                "catalog": streaming_config.catalog,
                "namespace": streaming_config.namespace,
                "kafka_servers": streaming_config.kafka_bootstrap_servers,
            },
            data_version=str(int(time.time())),
        )

        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
