#!/usr/bin/env python3
"""Spark Structured Streaming: Kafka -> Iceberg Bronze Tables with Dagster Pipes.

This script runs continuously, consuming events from Kafka topics
and writing them to Iceberg tables in the lakehouse catalog.

This script is orchestrated via Dagster Pipes - Dagster launches it via spark-submit
and monitors its progress via S3-based message passing.

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

Usage (via Dagster Pipes):
    This script is launched by Dagster using spark-submit. Dagster will:
    1. Upload this script to S3
    2. Pass Pipes bootstrap params via CLI args
    3. Launch spark-submit with proper configs
    4. Monitor progress via S3 message passing
"""

import time

import boto3

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
from pyspark.sql.types import (
    StructType,
)

from team_ops.schemas import SCHEMAS as TOPIC_SCHEMAS
from team_ops.spark_scripts.common import (
    create_spark_session,
    get_common_arg_parser,
)


def parse_args():
    """Parse command line arguments."""
    parser = get_common_arg_parser("Stream Kafka events to Iceberg Bronze tables")

    parser.add_argument(
        "--kafka-bootstrap-servers",
        default="kafka:9092",
        help="Kafka bootstrap servers",
    )

    parser.add_argument(
        "--checkpoint-path",
        default="s3a://checkpoints/streaming",
        help="Checkpoint location for streaming",
    )

    return parser.parse_known_args()


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
    spark: SparkSession, args, topic: str, schema: StructType, pipes_context
):
    """Create and start a streaming query for a topic."""
    table_name = f"{args.catalog}.{args.namespace}.bronze_{topic}"
    checkpoint_location = f"{args.checkpoint_path}/{topic}"

    pipes_context.log.info(f"Starting stream: {topic} -> {table_name}")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_bootstrap_servers)
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
    """Main entry point with Dagster Pipes integration."""
    args, _ = parse_args()

    with open_dagster_pipes(
        message_writer=PipesS3MessageWriter(client=boto3.client("s3")),
        context_loader=PipesS3ContextLoader(client=boto3.client("s3")),
        params_loader=PipesCliArgsParamsLoader(),
    ) as pipes:
        pipes.log.info("=" * 70)
        pipes.log.info("STREAMIFY - Kafka to Iceberg Streaming (Dagster Pipes)")
        pipes.log.info("=" * 70)
        pipes.log.info(f"Kafka:       {args.kafka_bootstrap_servers}")
        pipes.log.info(f"Polaris:     {args.polaris_uri}")
        pipes.log.info(f"Catalog:     {args.catalog}.{args.namespace}")
        pipes.log.info(f"Checkpoints: {args.checkpoint_path}")
        pipes.log.info("=" * 70)

        spark = create_spark_session("StreamToIceberg", args)
        spark.sparkContext.setLogLevel("WARN")

        create_namespace_if_not_exists(spark, args.catalog, args.namespace)

        queries = []
        for topic, schema in TOPIC_SCHEMAS.items():
            create_table_if_not_exists(spark, args.catalog, args.namespace, topic, schema)
            query = process_stream(spark, args, topic, schema, pipes)
            queries.append(query)
            pipes.log.info(f"✓ Stream started for {topic}")

        pipes.log.info("\n" + "=" * 70)
        pipes.log.info("All streams running. Monitoring...")
        pipes.log.info("=" * 70 + "\n")

        pipes.report_asset_materialization(
            metadata={
                "topics": {"raw_value": list(TOPIC_SCHEMAS.keys()), "type": "json"},
                "catalog": args.catalog,
                "namespace": args.namespace,
            },
            data_version=str(int(time.time())),
        )

        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
