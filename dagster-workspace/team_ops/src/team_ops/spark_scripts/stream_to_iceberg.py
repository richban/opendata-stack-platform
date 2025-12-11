#!/usr/bin/env python3
"""Spark Structured Streaming: Kafka -> Iceberg Bronze Tables.

This script runs continuously, consuming events from Kafka topics
and writing them to Iceberg tables in the lakehouse catalog.

Events:
- listen_events: Song play events
- page_view_events: Page navigation events
- auth_events: Authentication events

Each event gets:
- event_id: SHA256 hash for deduplication (userId + sessionId + ts + topic)
- event_date: Derived from ts for partitioning
- _processing_time: When the event was processed
- _kafka_partition: Source Kafka partition
- _kafka_offset: Source Kafka offset

Usage:
    spark-submit \\
        --master spark://spark-master:7077 \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,\\
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1,\\
org.apache.iceberg:iceberg-aws-bundle:1.7.1 \\
        stream_to_iceberg.py \\
        --kafka-bootstrap-servers kafka:9092 \\
        --polaris-uri http://polaris:8181/api/catalog \\
        --polaris-credential <client_id>:<client_secret> \\
        --checkpoint-path s3a://checkpoints/streaming
"""

import argparse
import hashlib
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    from_json,
    sha2,
    to_date,
    from_unixtime,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# Schema definitions (same as schemas.py but self-contained for spark-submit)
LISTEN_EVENTS_SCHEMA = StructType(
    [
        StructField("artist", StringType(), True),
        StructField("song", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("ts", LongType(), True),
        StructField("auth", StringType(), True),
        StructField("level", StringType(), True),
        StructField("city", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("state", StringType(), True),
        StructField("userAgent", StringType(), True),
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True),
        StructField("userId", LongType(), True),
        StructField("lastName", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("registration", LongType(), True),
        StructField("sessionId", IntegerType(), True),
        StructField("itemInSession", IntegerType(), True),
    ]
)

PAGE_VIEW_EVENTS_SCHEMA = StructType(
    [
        StructField("ts", LongType(), True),
        StructField("sessionId", IntegerType(), True),
        StructField("auth", StringType(), True),
        StructField("level", StringType(), True),
        StructField("itemInSession", IntegerType(), True),
        StructField("city", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("state", StringType(), True),
        StructField("userAgent", StringType(), True),
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True),
        StructField("userId", LongType(), True),
        StructField("lastName", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("registration", LongType(), True),
        StructField("page", StringType(), True),
    ]
)

AUTH_EVENTS_SCHEMA = StructType(
    [
        StructField("ts", LongType(), True),
        StructField("sessionId", IntegerType(), True),
        StructField("level", StringType(), True),
        StructField("itemInSession", IntegerType(), True),
        StructField("city", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("state", StringType(), True),
        StructField("userAgent", StringType(), True),
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True),
        StructField("userId", LongType(), True),
        StructField("lastName", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("registration", LongType(), True),
        StructField("success", StringType(), True),
    ]
)

TOPIC_SCHEMAS = {
    "listen_events": LISTEN_EVENTS_SCHEMA,
    "page_view_events": PAGE_VIEW_EVENTS_SCHEMA,
    "auth_events": AUTH_EVENTS_SCHEMA,
}


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Stream Kafka events to Iceberg Bronze tables"
    )
    parser.add_argument(
        "--kafka-bootstrap-servers",
        default="kafka:9092",
        help="Kafka bootstrap servers",
    )
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
        "--checkpoint-path",
        default="s3a://checkpoints/streaming",
        help="Checkpoint location for streaming",
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
    return parser.parse_args()


def create_spark_session(args) -> SparkSession:
    """Create SparkSession configured for Iceberg and Polaris."""
    return (
        SparkSession.builder.appName("StreamToIceberg")
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


def create_namespace_if_not_exists(spark: SparkSession, catalog: str, namespace: str):
    """Create the namespace if it doesn't exist."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")
    print(f"Namespace {catalog}.{namespace} ready")


def create_table_if_not_exists(
    spark: SparkSession, catalog: str, namespace: str, topic: str, schema: StructType
):
    """Create Iceberg table if it doesn't exist."""
    table_name = f"{catalog}.{namespace}.bronze_{topic}"

    # Build column definitions from schema
    columns = []
    for field in schema.fields:
        spark_type = field.dataType.simpleString()
        columns.append(f"{field.name} {spark_type}")

    # Add metadata columns
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
    print(f"Table {table_name} ready")


def process_stream(spark: SparkSession, args, topic: str, schema: StructType):
    """Create and start a streaming query for a topic."""
    table_name = f"{args.catalog}.{args.namespace}.bronze_{topic}"
    checkpoint_location = f"{args.checkpoint_path}/{topic}"

    print(f"Starting stream for {topic} -> {table_name}")

    # Read from Kafka
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON and add metadata columns
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
        # Add event_id for deduplication
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
        # Add event_date for partitioning (ts is in milliseconds)
        .withColumn("event_date", to_date(from_unixtime(col("ts") / 1000)))
        # Add processing timestamp
        .withColumn("_processing_time", current_timestamp())
    )

    # Write to Iceberg
    query = (
        parsed_df.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .option("fanout-enabled", "true")
        .toTable(table_name)
    )

    return query


def main():
    """Main entry point."""
    args = parse_args()

    print("=" * 70)
    print("STREAMIFY - Kafka to Iceberg Streaming")
    print("=" * 70)
    print(f"Kafka: {args.kafka_bootstrap_servers}")
    print(f"Polaris: {args.polaris_uri}")
    print(f"Catalog: {args.catalog}.{args.namespace}")
    print(f"Checkpoints: {args.checkpoint_path}")
    print("=" * 70)

    # Create Spark session
    spark = create_spark_session(args)
    spark.sparkContext.setLogLevel("WARN")

    # Create namespace
    create_namespace_if_not_exists(spark, args.catalog, args.namespace)

    # Create tables and start streams
    queries = []
    for topic, schema in TOPIC_SCHEMAS.items():
        # Create table
        create_table_if_not_exists(spark, args.catalog, args.namespace, topic, schema)

        # Start streaming query
        query = process_stream(spark, args, topic, schema)
        queries.append(query)
        print(f"Stream started for {topic}")

    print("\n" + "=" * 70)
    print("All streams started. Waiting for termination...")
    print("Press Ctrl+C to stop.")
    print("=" * 70 + "\n")

    # Wait for any stream to terminate
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
