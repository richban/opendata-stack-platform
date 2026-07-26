"""Declarative Streaming Pipeline for Streamify in Spark 4.2.

This module encapsulates Kafka stream ingestion, transformation specifications,
and target Iceberg table sink declarations into a declarative pipeline model.
"""

import logging
import sys

from pyspark.sql import DataFrame, SparkSession
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
    get_streaming_config,
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


class StreamifyDeclarativePipeline:
    """Declarative Spark 4.2 Streaming Pipeline for Streamify topics.

    Decouples source stream specifications, transformations, and sink target definitions
    from execution orchestration.
    """

    def __init__(self, spark: SparkSession, config: StreamingJobConfig):
        self.spark = spark
        self.config = config

    def declare_kafka_source(self, topic: str) -> DataFrame:
        """Declarative Kafka readStream source specification."""
        kafka_bootstrap = (
            self.config.kafka_bootstrap_servers.replace("localhost:9093", "kafka:9092")
            .replace("127.0.0.1:9093", "kafka:9092")
            .replace("localhost:9092", "kafka:9092")
        )
        logger.info(
            "Declaring Kafka source stream for topic '%s' (bootstrap=%s)...",
            topic,
            kafka_bootstrap,
        )
        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", 100000)
            .load()
        )

    def declare_transformations(
        self, source_df: DataFrame, schema: StructType
    ) -> DataFrame:
        """Declarative transformation specification.

        Parses JSON, generates event_id, and computes event_date.
        """
        parsed_df = (
            source_df.select(
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

    def declare_iceberg_sink(
        self,
        transformed_df: DataFrame,
        topic: str,
        trigger_interval: str = "30 seconds",
    ):
        """Declarative writeStream target sink specification for Iceberg."""
        table_name = f"{self.config.catalog}.{self.config.namespace}.bronze_{topic}"
        checkpoint_location = f"{self.config.checkpoint_path}/{topic}"

        logger.info(
            "Declaring Iceberg streaming sink -> Table: %s (Trigger: %s)...",
            table_name,
            trigger_interval,
        )

        return (
            transformed_df.writeStream.format("iceberg")
            .outputMode("append")
            .trigger(processingTime=trigger_interval)
            .option("checkpointLocation", checkpoint_location)
            .option("fanout-enabled", "true")
            .queryName(f"bronze_{topic}")
            .toTable(table_name)
        )

    def ensure_target_schema(self, topic: str, schema: StructType) -> None:
        """Declaratively verify namespace and table schemas exist in target catalog."""
        logger.info(
            "Ensuring Iceberg namespace '%s.%s' exists...",
            self.config.catalog,
            self.config.namespace,
        )
        create_namespace_if_not_exists(
            self.spark, self.config.catalog, self.config.namespace
        )

        logger.info(
            "Ensuring Iceberg table '%s.%s.bronze_%s' exists...",
            self.config.catalog,
            self.config.namespace,
            topic,
        )
        create_table_if_not_exists(
            self.spark,
            self.config.catalog,
            self.config.namespace,
            topic,
            schema,
        )

    def run_topic_stream(self, topic: str = "listen_events"):
        """Build and execute declarative streaming pipeline for a specified topic."""
        if topic not in TOPIC_SCHEMAS:
            raise ValueError(f"Schema not registered for topic '{topic}'")

        schema = TOPIC_SCHEMAS[topic]

        # 1. Target Schema Preparation
        self.ensure_target_schema(topic, schema)

        # 2. Pipeline Declarations
        source_df = self.declare_kafka_source(topic)
        transformed_df = self.declare_transformations(source_df, schema)
        streaming_query = self.declare_iceberg_sink(transformed_df, topic)

        logger.info(
            "✓ Declarative streaming query 'bronze_%s' started (Query ID: %s).",
            topic,
            streaming_query.id,
        )

        # 3. Lifecycle Management
        try:
            logger.info("Awaiting termination... Press Ctrl+C to stop.")
            streaming_query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received. Initiating stream shutdown...")
        finally:
            logger.info("Stopping streaming query...")
            streaming_query.stop()
            logger.info("✓ Streaming query stopped cleanly.")


def main():
    """Main execution entrypoint."""
    logger.info("Initializing S3 resource, Spark Session, and Streaming Config...")
    _s3_resource = create_s3_resource()
    spark = create_spark_session()
    streaming_config = get_streaming_config()

    logger.info("✓ Resources initialized & connected to Spark Connect.")
    logger.info(
        "Configuration: Kafka=%s | Catalog=%s.%s | Checkpoints=%s",
        streaming_config.kafka_bootstrap_servers,
        streaming_config.catalog,
        streaming_config.namespace,
        streaming_config.checkpoint_path,
    )

    pipeline = StreamifyDeclarativePipeline(spark, streaming_config)
    pipeline.run_topic_stream("listen_events")


if __name__ == "__main__":
    main()
