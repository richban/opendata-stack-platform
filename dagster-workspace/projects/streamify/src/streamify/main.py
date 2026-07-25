import logging
import sys

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

from streamify.defs.bronze_assets import (
    create_namespace_if_not_exists,
    create_table_if_not_exists,
)
from streamify.defs.resources import (
    StreamingJobConfig,
    create_s3_resource,
    create_spark_session,
    create_streaming_config,
)
from streamify.schemas import SCHEMAS as TOPIC_SCHEMAS, meta_schema

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("streamify.main")

# Initialize resources
logger.info("Initializing S3 resource, Spark Session, and Streaming Config...")
s3_resource = create_s3_resource()
spark = create_spark_session()
streaming_config = create_streaming_config()

logger.info("✓ Resources initialized & connected to Spark Connect.")
logger.info(
    "Configuration: Kafka=%s | Catalog=%s.%s | Checkpoints=%s",
    streaming_config.kafka_bootstrap_servers,
    streaming_config.catalog,
    streaming_config.namespace,
    streaming_config.checkpoint_path,
)

topic = "listen_events"
schema = TOPIC_SCHEMAS[topic]

# Ensure Iceberg namespace & table exist
logger.info(
    "Ensuring Iceberg namespace '%s.%s' exists...",
    streaming_config.catalog,
    streaming_config.namespace,
)
create_namespace_if_not_exists(
    spark, streaming_config.catalog, streaming_config.namespace
)

logger.info(
    "Ensuring Iceberg table '%s.%s.bronze_%s' exists...",
    streaming_config.catalog,
    streaming_config.namespace,
    topic,
)
create_table_if_not_exists(
    spark,
    streaming_config.catalog,
    streaming_config.namespace,
    topic,
    schema,
)


def process_stream(
    spark: SparkSession,
    streaming_config: StreamingJobConfig,
    topic: str,
    schema: StructType,
):
    """Transform Kafka stream: parse JSON, add event_id, extract event_date."""
    logger.info("Setting up Kafka readStream for topic '%s'...", topic)

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", streaming_config.kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 100000)
        .load()
    )

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

    metadata_cols = [field.name for field in meta_schema]
    data_cols = [field.name for field in schema.fields]

    return parsed_df.select(*data_cols, *metadata_cols)


listen_events_df = process_stream(spark, streaming_config, topic, schema)

table_name = f"{streaming_config.catalog}.{streaming_config.namespace}.bronze_{topic}"
checkpoint_location = f"{streaming_config.checkpoint_path}/{topic}"

logger.info("Starting streaming write query -> Table: %s (Trigger: 30s)...", table_name)

listen_events_stream = (
    listen_events_df.writeStream.format("iceberg")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", checkpoint_location)
    .option("fanout-enabled", "true")
    .queryName(f"bronze_{topic}")
    .toTable(table_name)
)

logger.info(
    "✓ Streaming query 'bronze_%s' started successfully (Query ID: %s).",
    topic,
    listen_events_stream.id,
)

try:
    logger.info("Awaiting termination... Press Ctrl+C to stop.")
    listen_events_stream.awaitTermination()
except KeyboardInterrupt:
    logger.info("KeyboardInterrupt received. Initiating stream shutdown...")
finally:
    logger.info("Stopping streaming query...")
    listen_events_stream.stop()
    logger.info("✓ Streaming query stopped cleanly.")
