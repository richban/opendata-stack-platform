import os
import time
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

from streamify.defs.resources import StreamingJobConfig
from streamify.schemas import SCHEMAS as TOPIC_SCHEMAS, meta_schema
from streamify.defs.bronze_assets import (
    create_namespace_if_not_exists,
    create_table_if_not_exists,
)

from streamify.defs.resources import (
    create_s3_resource,
    create_spark_session,
    create_streaming_config,
)
from streamify.schemas import SCHEMAS
from pyspark.sql.functions import col, from_json

# Initialize resources
s3_resource = create_s3_resource()
spark = create_spark_session()
streaming_config = create_streaming_config()

print("✓ Resources initialized and Spark session connected.")

schema = TOPIC_SCHEMAS["listen_events"]
topic = "listen_events"

create_namespace_if_not_exists(
    spark, streaming_config.catalog, streaming_config.namespace
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

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", streaming_config.kafka_bootstrap_servers)
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
        .withColumn("event_date", to_date(from_unixtime(col("ts") / 1000)))
        .withColumn("_processing_time", current_timestamp())
    )

    metadata_cols = [field.name for field in meta_schema]
    data_cols = [field.name for field in schema.fields]

    return parsed_df.select(*data_cols, *metadata_cols)


listen_events_df = process_stream(spark, streaming_config, topic, schema)

table_name = f"{streaming_config.catalog}.{streaming_config.namespace}.bronze_{topic}"
checkpoint_location = f"{streaming_config.checkpoint_path}/{topic}"

listen_events_stream = (
    listen_events_df.writeStream.format("iceberg")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", checkpoint_location)
    .option("fanout-enabled", "true")
    .queryName(f"bronze_{topic}")
    .toTable(table_name)
)

# Wait for 5 minutes, then stop the stream programmatically
try:
    listen_events_stream.awaitTermination(
        timeout=300
    )  # Returns True if query terminated on its own, False if timeout
finally:
    print("Stopping query...")
    listen_events_stream.stop()
