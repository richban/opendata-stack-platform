import logging
import sys

import redis

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    from_json,
    lit,
    sha2,
    to_date,
    udf,
)
from pyspark.sql.types import StringType, StructType

from streamify.defs.bronze_assets import (
    create_namespace_if_not_exists,
    create_table_if_not_exists,
)
from streamify.defs.resources import (
    StreamingJobConfig,
    create_clickhouse_resource,
    create_s3_resource,
    create_spark_session,
    get_streaming_config,
)
from streamify.schemas import (
    SCHEMAS as TOPIC_SCHEMAS,
    meta_schema,
)

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("streamify.main")

# ClickHouse JDBC driver class
CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"


def _string_decode_fn(s: str, encoding: str = "utf-8") -> str:
    """Decode unicode/octal-escaped strings."""
    if s:
        try:
            return (
                s.encode("latin1")
                .decode("unicode-escape")
                .encode("latin1")
                .decode(encoding)
                .strip('"')
            )
        except Exception:
            return s
    return s


# ------------------------------------------------------------------
# ClickHouse DDL bootstrap via clickhouse-connect
# ------------------------------------------------------------------
def ensure_clickhouse_table_exists(
    config: StreamingJobConfig,
) -> None:
    """Create ClickHouse DB and ReplacingMergeTree table."""
    logger.info(
        "Ensuring ClickHouse table '%s.silver_playback_events' exists (host=%s:%d)...",
        config.clickhouse_db,
        config.clickhouse_host,
        config.clickhouse_port,
    )

    client = create_clickhouse_resource(config)
    try:
        client.command(f"CREATE DATABASE IF NOT EXISTS {config.clickhouse_db}")
        logger.info("✓ ClickHouse database '%s' ensured.", config.clickhouse_db)

        client.command(f"""
            CREATE TABLE IF NOT EXISTS
            {config.clickhouse_db}.silver_playback_events (
                event_id String,
                user_id UInt64,
                artist String,
                song String,
                duration Float64,
                event_ts DateTime64(3),
                session_id String,
                city String,
                state String,
                enriched_first_name String,
                enriched_last_name String,
                enriched_gender String,
                enriched_city String,
                enriched_state String,
                enriched_zip String,
                _processing_time DateTime64(3)
            ) ENGINE = ReplacingMergeTree(event_ts)
            ORDER BY (state, toYYYYMMDD(event_ts), event_id)
            SETTINGS index_granularity = 8192
        """)
        logger.info("✓ ClickHouse table 'silver_playback_events' ensured.")
    finally:
        client.close()


# ------------------------------------------------------------------
# Redis batch lookup helper
# ------------------------------------------------------------------
def _redis_batch_lookup(
    user_ids: list[int],
    redis_host: str,
    redis_port: int,
) -> dict[int, dict[str, str]]:
    """Pipeline HGETALL for a list of user_ids against Redis."""
    profile_map: dict[int, dict[str, str]] = {}
    if not user_ids:
        return profile_map

    r_client = redis.Redis(
        host=redis_host,
        port=redis_port,
        decode_responses=True,
    )
    try:
        with r_client.pipeline(transaction=False) as pipe:
            for uid in user_ids:
                pipe.hgetall(f"user:{uid}")
            results = pipe.execute()
            for uid, prof in zip(user_ids, results):
                if prof:
                    profile_map[uid] = prof
    finally:
        r_client.close()
    return profile_map


# ------------------------------------------------------------------
# foreachBatch sink: Redis enrich → JDBC write to ClickHouse
# ------------------------------------------------------------------
def make_clickhouse_redis_sink(
    spark: SparkSession,
    config: StreamingJobConfig,
):
    """foreachBatch handler: Redis enrichment → JDBC write to ClickHouse."""
    redis_host = config.redis_host
    redis_port = config.redis_port
    ch_jdbc_url = config.clickhouse_jdbc_url
    ch_table = "silver_playback_events"

    jdbc_props = {
        "driver": CLICKHOUSE_DRIVER,
        "user": config.clickhouse_user,
        "password": config.clickhouse_password,
        "batchsize": "10000",
        "isolationLevel": "NONE",
    }

    def write_batch(batch_df: DataFrame, batch_id: int):
        if batch_df.isEmpty():
            return

        # --- 1. Collect distinct user_ids on the driver ---
        uid_rows = batch_df.select("userId").distinct().collect()
        user_ids = [r["userId"] for r in uid_rows if r["userId"] is not None]

        # --- 2. Pipelined Redis lookup (single RTT) ---
        profile_map = _redis_batch_lookup(user_ids, redis_host, redis_port)
        logger.info(
            "Batch %d: Redis lookup %d/%d profiles hit.",
            batch_id,
            len(profile_map),
            len(user_ids),
        )

        # --- 3. Build a broadcast-sized profiles DF and join ---
        if profile_map:
            profile_rows = [
                (
                    uid,
                    prof.get("first_name", ""),
                    prof.get("last_name", ""),
                    prof.get("gender", ""),
                    prof.get("city", ""),
                    prof.get("state", ""),
                    prof.get("zip_code", ""),
                )
                for uid, prof in profile_map.items()
            ]
            profiles_df = spark.createDataFrame(
                profile_rows,
                schema=[
                    "p_userId",
                    "enriched_first_name",
                    "enriched_last_name",
                    "enriched_gender",
                    "enriched_city",
                    "enriched_state",
                    "enriched_zip",
                ],
            )
            enriched_df = batch_df.join(
                profiles_df,
                batch_df["userId"] == profiles_df["p_userId"],
                "left",
            ).drop("p_userId")
        else:
            # No profiles found — add empty enrichment cols
            enriched_df = batch_df
            for f in [
                "enriched_first_name",
                "enriched_last_name",
                "enriched_gender",
                "enriched_city",
                "enriched_state",
                "enriched_zip",
            ]:
                enriched_df = enriched_df.withColumn(f, lit(""))

        # Fill nulls from left-join misses
        for f in [
            "enriched_first_name",
            "enriched_last_name",
            "enriched_gender",
            "enriched_city",
            "enriched_state",
            "enriched_zip",
        ]:
            enriched_df = enriched_df.fillna({f: ""})

        # --- 4. Project to ClickHouse target schema ---
        out_df = enriched_df.select(
            col("event_id"),
            col("userId").alias("user_id"),
            col("artist"),
            col("song"),
            col("duration"),
            col("event_ts"),
            col("sessionId").cast("string").alias("session_id"),
            col("city"),
            col("state"),
            col("enriched_first_name"),
            col("enriched_last_name"),
            col("enriched_gender"),
            col("enriched_city"),
            col("enriched_state"),
            col("enriched_zip"),
            col("_processing_time"),
        )

        # --- 5. JDBC write (distributed across executors) ---
        (
            out_df.write.format("jdbc")
            .option("url", ch_jdbc_url)
            .option("dbtable", ch_table)
            .option("driver", jdbc_props["driver"])
            .option("user", jdbc_props["user"])
            .option("password", jdbc_props["password"])
            .option("batchsize", jdbc_props["batchsize"])
            .option(
                "isolationLevel",
                jdbc_props["isolationLevel"],
            )
            .mode("append")
            .save()
        )

        logger.info(
            "✓ Batch %d: wrote %d enriched rows to ClickHouse via JDBC.",
            batch_id,
            out_df.count(),
        )

    return write_batch


# ------------------------------------------------------------------
# Pipeline class
# ------------------------------------------------------------------
class StreamifyDeclarativePipeline:
    """Declarative Spark Streaming Pipeline for Streamify.

    Manages dual sinks: Iceberg lakehouse + ClickHouse fast-path.
    """

    def __init__(self, spark: SparkSession, config: StreamingJobConfig):
        self.spark = spark
        self.config = config

    def declare_kafka_source(self, topic: str) -> DataFrame:
        """Kafka readStream source specification."""
        bootstrap = self.config.get_kafka_bootstrap_servers()
        logger.info(
            "Declaring Kafka source for '%s' (bootstrap=%s)...",
            topic,
            bootstrap,
        )
        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", 100000)
            .load()
        )

    def declare_transformations(
        self, source_df: DataFrame, schema: StructType
    ) -> DataFrame:
        """Parse JSON, generate event_id, compute event_date."""
        string_decode = udf(_string_decode_fn, StringType())

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
            .withColumn(
                "event_ts",
                (col("ts") / 1000).cast("timestamp"),
            )
            .withColumn("event_date", to_date(col("event_ts")))
            .withColumn("_processing_time", current_timestamp())
            .withColumn("song", string_decode(col("song")))
            .withColumn("artist", string_decode(col("artist")))
        )

        metadata_cols = [f.name for f in meta_schema]
        data_cols = [f.name for f in schema.fields]
        return parsed_df.select(*data_cols, *metadata_cols)

    def declare_iceberg_sink(
        self,
        transformed_df: DataFrame,
        topic: str,
        trigger_interval: str = "30 seconds",
    ):
        """writeStream sink → Iceberg lakehouse."""
        table = f"{self.config.catalog}.{self.config.namespace}.bronze_{topic}"
        chkpt = f"{self.config.checkpoint_path}/{topic}"

        logger.info(
            "Declaring Iceberg sink → %s (trigger=%s)...",
            table,
            trigger_interval,
        )
        return (
            transformed_df.writeStream.format("iceberg")
            .outputMode("append")
            .trigger(processingTime=trigger_interval)
            .option("checkpointLocation", chkpt)
            .option("fanout-enabled", "true")
            .queryName(f"bronze_{topic}")
            .toTable(table)
        )

    def declare_clickhouse_sink(
        self,
        transformed_df: DataFrame,
        topic: str,
        trigger_interval: str = "10 seconds",
    ):
        """writeStream sink → ClickHouse via JDBC with Redis enrichment."""
        chkpt = f"{self.config.checkpoint_path}/{topic}_clickhouse"
        sink_fn = make_clickhouse_redis_sink(self.spark, self.config)

        logger.info(
            "Declaring ClickHouse JDBC sink → %s (trigger=%s)...",
            self.config.clickhouse_db,
            trigger_interval,
        )
        return (
            transformed_df.writeStream.trigger(processingTime=trigger_interval)
            .option("checkpointLocation", chkpt)
            .queryName(f"clickhouse_{topic}")
            .foreachBatch(sink_fn)
            .start()
        )

    def ensure_target_schema(self, topic: str, schema: StructType) -> None:
        """Ensure Iceberg namespace and table exist."""
        logger.info(
            "Ensuring Iceberg namespace '%s.%s'...",
            self.config.catalog,
            self.config.namespace,
        )
        create_namespace_if_not_exists(
            self.spark,
            self.config.catalog,
            self.config.namespace,
        )

        logger.info(
            "Ensuring Iceberg table '%s.%s.bronze_%s'...",
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
        """Launch dual streaming pipeline for a topic."""
        if topic not in TOPIC_SCHEMAS:
            raise ValueError(f"Schema not registered for topic '{topic}'")

        schema = TOPIC_SCHEMAS[topic]

        # 1. Target schema preparation
        self.ensure_target_schema(topic, schema)
        ensure_clickhouse_table_exists(self.config)

        # 2. Pipeline declarations
        source_df = self.declare_kafka_source(topic)
        transformed_df = self.declare_transformations(source_df, schema)

        # Dual sinks
        iceberg_q = self.declare_iceberg_sink(transformed_df, topic)
        clickhouse_q = self.declare_clickhouse_sink(transformed_df, topic)

        logger.info(
            "✓ Streams started: Iceberg=%s, ClickHouse=%s.",
            iceberg_q.id,
            clickhouse_q.id,
        )

        # 3. Lifecycle
        try:
            logger.info("Awaiting termination... Ctrl+C to stop.")
            self.spark.streams.awaitAnyTermination()
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received.")
        finally:
            logger.info("Stopping streaming queries...")
            iceberg_q.stop()
            clickhouse_q.stop()
            logger.info("✓ Queries stopped cleanly.")


def main():
    """Main entrypoint."""
    logger.info("Initializing S3, Spark Session, and Config...")
    _s3_resource = create_s3_resource()
    spark = create_spark_session()
    cfg = get_streaming_config()

    logger.info("✓ Resources initialized.")
    logger.info(
        "Kafka=%s | Catalog=%s.%s | CH=%s:%d | Redis=%s:%d",
        cfg.kafka_bootstrap_servers,
        cfg.catalog,
        cfg.namespace,
        cfg.clickhouse_host,
        cfg.clickhouse_port,
        cfg.redis_host,
        cfg.redis_port,
    )

    pipeline = StreamifyDeclarativePipeline(spark, cfg)
    pipeline.run_topic_stream("listen_events")


if __name__ == "__main__":
    main()
