"""
Marimo notebook for testing Kafka -> Spark Streaming -> Iceberg pipeline independently.

Run with: marimo edit notebooks/test_streaming.py
"""

import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import os

    import marimo as mo

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

    return (
        SparkSession,
        StructType,
        col,
        concat_ws,
        current_timestamp,
        from_json,
        from_unixtime,
        mo,
        os,
        sha2,
        to_date,
    )


@app.cell
def _(mo, os):
    # Load environment variables
    polaris_client_id = os.getenv("POLARIS_CLIENT_ID")
    polaris_client_secret = os.getenv("POLARIS_CLIENT_SECRET")
    polaris_uri = os.getenv("POLARIS_URI", "http://localhost:8181")
    catalog_name = os.getenv("POLARIS_CATALOG", "lakehouse")

    mo.md(f"""
    ## Configuration

    - Polaris URI: `{polaris_uri}`
    - Catalog: `{catalog_name}`
    - Client ID: `{polaris_client_id[:8]}...`
    """)
    return catalog_name, polaris_client_id, polaris_client_secret, polaris_uri


@app.cell
def _(
    SparkSession,
    catalog_name,
    os,
    polaris_client_id,
    polaris_client_secret,
    polaris_uri,
):
    # Create Spark session with Spark Connect
    polaris_credential = f"{polaris_client_id}:{polaris_client_secret}"

    # Docker hostname translation
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

    print(f"✓ Connected to Spark Connect: {spark.version}")
    spark
    return (spark,)


@app.cell
def _(mo):
    mo.md("""
    ## Define Schemas

    Using the same schemas from `streamify.schemas`
    """)
    return


@app.cell
def _(StructType):
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
    )

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

    print(f"✓ Schema loaded with {len(LISTEN_EVENTS_SCHEMA.fields)} fields")
    LISTEN_EVENTS_SCHEMA
    return IntegerType, LISTEN_EVENTS_SCHEMA, LongType, StringType, StructField


@app.cell
def _(mo):
    mo.md("""
    ## Test 1: Read from Kafka (Streaming)

    Read a streaming DataFrame from Kafka to verify connectivity.
    """)
    return


@app.cell
def _(spark):
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "listen_events")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    print("✓ Kafka streaming DataFrame created")
    print(f"Schema: {kafka_df.schema}")
    kafka_df
    return (kafka_df,)


@app.cell
def _(mo):
    mo.md("""
    ## Test 2: Parse JSON and Add Metadata

    Transform the Kafka stream following the exact pattern from `process_stream()`.
    """)
    return


@app.cell
def _(
    LISTEN_EVENTS_SCHEMA,
    col,
    concat_ws,
    current_timestamp,
    from_json,
    from_unixtime,
    kafka_df,
    sha2,
    to_date,
):
    # Parse JSON, flatten struct, and add metadata
    parsed_df = (
        kafka_df.select(
            from_json(col("value").cast("string"), LISTEN_EVENTS_SCHEMA).alias("data"),
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

    print("✓ Parsed DataFrame created")
    print(f"Schema: {parsed_df.schema}")
    parsed_df
    return (parsed_df,)


@app.cell
def _(mo):
    mo.md("""
    ## Test 3: Create Iceberg Table

    Create the bronze table using the Spark Catalog API.
    """)
    return


@app.cell
def _(
    IntegerType,
    LISTEN_EVENTS_SCHEMA,
    LongType,
    StringType,
    StructField,
    StructType,
    catalog_name,
    spark,
):
    from pyspark.sql.types import DateType, TimestampType

    table_name = f"{catalog_name}.streamify.bronze_listen_events"

    # Create namespace
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.streamify")
        print(f"✓ Namespace {catalog_name}.streamify created/verified")
    except Exception as e:
        print(f"Namespace already exists or error: {e}")

    # Check if table exists
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        print(f"✓ Table {table_name} already exists")
        table_exists = True
    except Exception:
        print(f"Table {table_name} does not exist, will create")
        table_exists = False

    # Create table if it doesn't exist
    if not table_exists:
        extended_fields = list(LISTEN_EVENTS_SCHEMA.fields) + [
            StructField("_kafka_partition", IntegerType(), True),
            StructField("_kafka_offset", LongType(), True),
            StructField("_kafka_timestamp", TimestampType(), True),
            StructField("event_id", StringType(), True),
            StructField("event_date", DateType(), True),
            StructField("_processing_time", TimestampType(), True),
        ]
        extended_schema = StructType(extended_fields)

        spark.catalog.createTable(
            tableName=table_name,
            schema=extended_schema,
            source="iceberg",
            partitioningColumns=["event_date"],
            description="Bronze streaming table for listen_events (test)",
        )
        print(f"✓ Table {table_name} created with {len(extended_fields)} columns")
        print("  Partitioned by: event_date")

    table_name
    return (table_name,)


@app.cell
def _(mo):
    mo.md("""
    ## Test 4: Write Stream to Iceberg (Console Test)

    Write the stream to console first to verify the data pipeline works.
    This will run for 30 seconds and then stop.
    """)
    return


@app.cell
def _(mo):
    test_console = mo.ui.run_button(label="Run Console Test (30s)")
    test_console
    return (test_console,)


@app.cell
def _(parsed_df, test_console):
    if test_console.value:
        console_query = (
            parsed_df.writeStream.outputMode("append")
            .format("console")
            .option("truncate", "false")
            .option("numRows", "10")
            .start()
        )

        print("✓ Console stream started. Waiting 30 seconds...")
        import time

        time.sleep(30)
        console_query.stop()
        print("✓ Console stream stopped")
    return


@app.cell
def _(mo):
    mo.md("""
    ## Test 5: Write Stream to Iceberg (Real)

    Write the stream to Iceberg table with 30-second micro-batches.

    ⚠️ **Warning:** This will run indefinitely until you stop it manually.
    """)
    return


@app.cell
def _(mo):
    test_iceberg = mo.ui.run_button(label="Start Iceberg Streaming (Runs Forever)")
    test_iceberg
    return (test_iceberg,)


@app.cell
def _(parsed_df, table_name, test_iceberg):
    if test_iceberg.value:
        checkpoint_location = "s3a://checkpoints/streaming/listen_events_test"

        iceberg_query = (
            parsed_df.writeStream.format("iceberg")
            .outputMode("append")
            .trigger(processingTime="30 seconds")
            .option("checkpointLocation", checkpoint_location)
            .option("fanout-enabled", "true")
            .toTable(table_name)
        )

        print("✓ Iceberg stream started")
        print(f"  Table: {table_name}")
        print(f"  Checkpoint: {checkpoint_location}")
        print("  Write interval: 30 seconds")
        print("\n⚠️ Stream is running. Go to Spark UI to monitor:")
        print("  http://localhost:8080")
        print("\nTo stop: Restart this cell or stop the notebook")
    return


@app.cell
def _(mo):
    mo.md("""
    ## Test 6: Query the Table

    After running the stream for a bit, query the table to verify data is being written.
    """)
    return


@app.cell
def _(mo):
    query_button = mo.ui.run_button(label="Query Table")
    query_button
    return (query_button,)


@app.cell
def _(query_button, spark, table_name):
    if query_button.value:
        result_df = spark.sql(f"SELECT * FROM {table_name} LIMIT 10")
        print(f"✓ Query results from {table_name}:")
        result_df.show(truncate=False)

        count = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0][
            "count"
        ]
        print(f"\nTotal rows: {count:,}")
    return


@app.cell
def _(mo):
    mo.md("""
    ## Cleanup

    Drop the test table if needed.
    """)
    return


@app.cell
def _(mo):
    cleanup_button = mo.ui.run_button(label="Drop Test Table")
    cleanup_button
    return (cleanup_button,)


@app.cell
def _(cleanup_button, spark, table_name):
    if cleanup_button.value:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        print(f"✓ Dropped table {table_name}")
    return


if __name__ == "__main__":
    app.run()
