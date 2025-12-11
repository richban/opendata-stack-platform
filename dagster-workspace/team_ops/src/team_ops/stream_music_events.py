import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, from_json, hour, month, year

from team_ops.schemas import SCHEMAS


def create_spark_session(app_name: str = "StreamifyMusicEvents") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )


def create_kafka_read_stream(
    spark: SparkSession,
    kafka_address: str,
    kafka_port: str,
    topic: str,
    starting_offset: str = "earliest",
):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", f"{kafka_address}:{kafka_port}")
        .option("subscribe", topic)
        .option("startingOffsets", starting_offset)
        .load()
    )


def process_stream(stream, schema, topic: str):
    processed = (
        stream.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
        .withColumn("ts_timestamp", (col("ts") / 1000).cast("timestamp"))
        .withColumn("year", year(col("ts_timestamp")))
        .withColumn("month", month(col("ts_timestamp")))
        .withColumn("day", dayofmonth(col("ts_timestamp")))
        .withColumn("hour", hour(col("ts_timestamp")))
    )
    return processed


def create_file_write_stream(
    stream,
    storage_path: str,
    checkpoint_path: str,
    trigger: str = "120 seconds",
):
    return (
        stream.writeStream.format("parquet")
        .partitionBy("month", "day", "hour")
        .option("path", storage_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger)
        .outputMode("append")
    )


def run_streaming_pipeline(
    kafka_address: str = "kafka",
    kafka_port: str = "9092",
    storage_base_path: str = "/data/lake",
    checkpoint_base_path: str = "/data/checkpoints",
):
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(
        f"Starting music streaming pipeline: kafka={kafka_address}:{kafka_port}, storage={storage_base_path}"
    )

    streams = {}
    writers = {}

    for topic, schema in SCHEMAS.items():
        print(f"Setting up stream for topic: {topic}")

        raw_stream = create_kafka_read_stream(spark, kafka_address, kafka_port, topic)

        processed_stream = process_stream(raw_stream, schema, topic)

        writer = create_file_write_stream(
            processed_stream,
            f"{storage_base_path}/{topic}",
            f"{checkpoint_base_path}/{topic}",
        )

        streams[topic] = processed_stream
        writers[topic] = writer

    print("Starting all streaming writers...")
    for topic, writer in writers.items():
        print(f"Starting writer for {topic}")
        writer.start()

    print("All streams started. Waiting for termination...")
    spark.streams.awaitAnyTermination()


def main():
    parser = argparse.ArgumentParser(description="Streamify Music Events Pipeline")
    parser.add_argument("--kafka-address", default="kafka", help="Kafka broker address")
    parser.add_argument("--kafka-port", default="9092", help="Kafka broker port")
    parser.add_argument(
        "--storage-path",
        default="/data/lake",
        help="Base path for data lake storage",
    )
    parser.add_argument(
        "--checkpoint-path",
        default="/data/checkpoints",
        help="Base path for checkpoints",
    )

    args = parser.parse_args()

    run_streaming_pipeline(
        kafka_address=args.kafka_address,
        kafka_port=args.kafka_port,
        storage_base_path=args.storage_path,
        checkpoint_base_path=args.checkpoint_path,
    )


if __name__ == "__main__":
    main()
