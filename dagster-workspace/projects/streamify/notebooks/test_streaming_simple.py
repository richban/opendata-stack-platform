#!/usr/bin/env python3
"""
Simple test script for Kafka -> Spark Streaming -> Iceberg pipeline.

Run with: python notebooks/test_streaming_simple.py

This tests the pipeline independently from Dagster to debug schema issues.
"""

import os
import sys
from pathlib import Path

# Add parent directory to path to import streamify modules
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

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
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
    DateType,
    DoubleType,
)


def create_spark_session():
    """Create Spark session with Iceberg/Polaris configuration."""
    polaris_client_id = os.getenv("POLARIS_CLIENT_ID")
    polaris_client_secret = os.getenv("POLARIS_CLIENT_SECRET")
    polaris_uri = os.getenv("POLARIS_URI", "http://localhost:8181")
    catalog_name = os.getenv("POLARIS_CATALOG", "lakehouse")

    if not polaris_client_id or not polaris_client_secret:
        raise ValueError("POLARIS_CLIENT_ID and POLARIS_CLIENT_SECRET must be set")

    polaris_credential = f"{polaris_client_id}:{polaris_client_secret}"
    config_polaris_uri = polaris_uri.replace("localhost", "polaris")

    spark = (
        SparkSession.builder.appName("StreamingTest")
        .remote("sc://localhost:15002")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(f"spark.sql.catalog.{catalog_name}.type", "rest")
        .config(f"spark.sql.catalog.{catalog_name}.uri", config_polaris_uri)
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", catalog_name)
        .config(f"spark.sql.catalog.{catalog_name}.credential", polaris_credential)
        .config(
            f"spark.sql.catalog.{catalog_name}.oauth2-server-uri",
            f"{config_polaris_uri}/v1/oauth/tokens",
        )
        .config(f"spark.sql.catalog.{catalog_name}.scope", "PRINCIPAL_ROLE:ALL")
        .config(f"spark.sql.catalog.{catalog_name}.s3.endpoint", "http://minio:9000")
        .config(
            f"spark.sql.catalog.{catalog_name}.s3.access-key-id",
            os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        )
        .config(
            f"spark.sql.catalog.{catalog_name}.s3.secret-access-key",
            os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        )
        .config(f"spark.sql.catalog.{catalog_name}.s3.path-style-access", "true")
        .config("spark.sql.defaultCatalog", catalog_name)
        .getOrCreate()
    )

    print(f"✓ Connected to Spark Connect (version {spark.version})")
    print(f"  Polaris URI: {config_polaris_uri}")
    print(f"  Catalog: {catalog_name}")

    return spark, catalog_name


def get_listen_events_schema():
    """Return the schema for listen_events topic."""
    return StructType(
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


def test_kafka_read(spark):
    """Test reading from Kafka."""
    print("\n=== Test 1: Read from Kafka ===")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "listen_events")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    print(f"✓ Kafka streaming DataFrame created")
    print(f"  Schema: {kafka_df.schema}")

    return kafka_df


def test_parse_transform(kafka_df, schema):
    """Test parsing JSON and transforming data."""
    print("\n=== Test 2: Parse JSON and Transform ===")

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
        .withColumn("event_date", to_date(from_unixtime(col("ts") / 1000)))
        .withColumn("_processing_time", current_timestamp())
    )

    print(f"✓ Parsed DataFrame created")
    print(f"  Schema: {parsed_df.schema}")
    print(f"  Columns: {', '.join(parsed_df.columns)}")

    return parsed_df


def test_create_table(spark, catalog_name, schema):
    """Test creating Iceberg table."""
    print("\n=== Test 3: Create Iceberg Table ===")

    table_name = f"{catalog_name}.streamify.bronze_listen_events_test"

    # Create namespace
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.streamify")
        print(f"✓ Namespace {catalog_name}.streamify ready")
    except Exception as e:
        print(f"  Note: {e}")

    # Check if table exists
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        print(f"✓ Table {table_name} already exists")
        return table_name, True
    except Exception:
        print(f"  Table {table_name} does not exist")

    # Create table
    extended_fields = list(schema.fields) + [
        StructField("_kafka_partition", IntegerType(), True),
        StructField("_kafka_offset", LongType(), True),
        StructField("_kafka_timestamp", TimestampType(), True),
        StructField("event_id", StringType(), True),
        StructField("event_date", DateType(), True),
        StructField("_processing_time", TimestampType(), True),
    ]
    extended_schema = StructType(extended_fields)

    print(f"  Creating table with {len(extended_fields)} columns...")
    print(f"  Columns: {', '.join([f.name for f in extended_fields])}")

    spark.catalog.createTable(
        tableName=table_name,
        schema=extended_schema,
        source="iceberg",
        partitioningColumns=["event_date"],
        description="Test table for streaming pipeline",
    )

    print(f"✓ Table {table_name} created")
    print(f"  Partitioned by: event_date")

    return table_name, False


def test_console_stream(parsed_df):
    """Test streaming to console."""
    print("\n=== Test 4: Stream to Console (10 seconds) ===")

    query = (
        parsed_df.writeStream.outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("numRows", "5")
        .start()
    )

    print("✓ Console stream started. Waiting 10 seconds...")

    import time

    time.sleep(10)

    query.stop()
    print("✓ Console stream stopped")


def main():
    """Run all tests."""
    print("=" * 60)
    print("Kafka → Spark Streaming → Iceberg Test")
    print("=" * 60)

    # Create Spark session
    spark, catalog_name = create_spark_session()

    # Get schema
    schema = get_listen_events_schema()
    print(f"\n✓ Schema loaded with {len(schema.fields)} fields")

    # Test 1: Read from Kafka
    kafka_df = test_kafka_read(spark)

    # Test 2: Parse and transform
    parsed_df = test_parse_transform(kafka_df, schema)

    # Test 3: Create table
    table_name, exists = test_create_table(spark, catalog_name, schema)

    # Test 4: Console stream
    test_console_stream(parsed_df)

    print("\n" + "=" * 60)
    print("All tests passed! ✓")
    print("=" * 60)
    print(f"\nNext steps:")
    print(f"1. Check Polaris Console: http://localhost:3001")
    print(f"   - Should see table: {table_name}")
    print(f"2. To start real streaming to Iceberg, uncomment the code below")
    print(f"3. Monitor Spark UI: http://localhost:8080")

    # Uncomment to start real streaming
    # print("\n=== Starting Iceberg Stream ===")
    # checkpoint_location = "s3a://checkpoints/streaming/listen_events_test"
    # query = (
    #     parsed_df
    #     .writeStream
    #     .format("iceberg")
    #     .outputMode("append")
    #     .trigger(processingTime="30 seconds")
    #     .option("checkpointLocation", checkpoint_location)
    #     .option("fanout-enabled", "true")
    #     .toTable(table_name)
    # )
    # print(f"✓ Streaming to {table_name}")
    # print(f"  Press Ctrl+C to stop")
    # spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
