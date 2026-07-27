import logging
import sys

from collections.abc import Iterator

import pandas as pd
import pyarrow as pa

from cachetools import TTLCache
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    broadcast,
    col,
    concat_ws,
    current_timestamp,
    from_json,
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
    get_executor_redis_client,
    get_streaming_config,
)
from streamify.schemas import (
    ENRICHED_USER_PROFILE_SCHEMA,
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
                song_year String,
                artist_location String,
                _processing_time DateTime64(3)
            ) ENGINE = ReplacingMergeTree(event_ts)
            ORDER BY (state, toYYYYMMDD(event_ts), event_id)
            SETTINGS index_granularity = 8192
        """)
        logger.info("✓ ClickHouse table 'silver_playback_events' ensured.")
    finally:
        client.close()


# ------------------------------------------------------------------
# Executor-side mapInPandas logic
# ------------------------------------------------------------------
_executor_profile_cache = TTLCache(maxsize=10000, ttl=600)

def _enrich_profiles_partition(
    batches: Iterator[pa.RecordBatch],
    redis_host: str,
    redis_port: int,
) -> Iterator[pa.RecordBatch]:
    """PyArrow partition iterator for executor-side Redis lookups.
    
    Why this pattern?
    1. Executor-side lookup: By using mapInArrow, we push the Redis lookups down to 
       the distributed Spark worker nodes rather than bottlenecking the single driver JVM.
    2. Pure Arrow Iterator: Spark partitions the data and sends Apache Arrow batches. 
       By receiving and yielding pure `pyarrow.RecordBatch` objects, we eliminate the 
       costly Arrow <-> Pandas conversion overhead completely (zero-copy).
    3. TTLCache: We maintain a local LRU cache in the Python worker memory to avoid 
       network I/O to Redis for active users who appear multiple times within the TTL.
    4. Pipelined I/O: Cache misses are collected and fetched in a single pipelined Redis 
       network round-trip to drastically reduce latency.
    """
    r_client = get_executor_redis_client(redis_host, redis_port)
    cols_to_keep = ENRICHED_USER_PROFILE_SCHEMA.fieldNames()

    for batch in batches:
        if batch.num_rows == 0:
            yield batch
            continue

        user_ids = batch.column("userId").to_pylist()

        missing_ids = list({
            uid for uid in user_ids 
            if uid is not None and uid not in _executor_profile_cache
        })

        if missing_ids:
            with r_client.pipeline(transaction=False) as pipe:
                for uid in missing_ids:
                    pipe.hgetall(f"user:{uid}")
                results = pipe.execute()
                for uid, prof in zip(missing_ids, results):
                    _executor_profile_cache[uid] = prof or {}

        fn_list, ln_list, gen_list = [], [], []
        city_list, state_list, zip_list = [], [], []
        
        for uid in user_ids:
            prof = _executor_profile_cache.get(uid, {}) if uid is not None else {}
            fn_list.append(prof.get("first_name", ""))
            ln_list.append(prof.get("last_name", ""))
            gen_list.append(prof.get("gender", ""))
            city_list.append(prof.get("city", ""))
            state_list.append(prof.get("state", ""))
            zip_list.append(prof.get("zip_code", ""))

        arr_fn = pa.array(fn_list, type=pa.string())
        arr_ln = pa.array(ln_list, type=pa.string())
        arr_gen = pa.array(gen_list, type=pa.string())
        arr_city = pa.array(city_list, type=pa.string())
        arr_state = pa.array(state_list, type=pa.string())
        arr_zip = pa.array(zip_list, type=pa.string())

        new_arrays = [
            *batch.columns,
            arr_fn, arr_ln, arr_gen, arr_city, arr_state, arr_zip
        ]
        new_names = [*batch.schema.names, *cols_to_keep]
        
        yield pa.RecordBatch.from_arrays(new_arrays, names=new_names)


# ------------------------------------------------------------------
# foreachBatch sink: pure JDBC write to ClickHouse
# ------------------------------------------------------------------
def make_clickhouse_sink(config: StreamingJobConfig):
    """foreachBatch handler: write to ClickHouse via clickhouse-connect."""
    # Map localhost to clickhouse container name for executor
    ch_host = config.clickhouse_host.replace("localhost", "clickhouse").replace("127.0.0.1", "clickhouse")
    ch_port = config.clickhouse_port
    ch_user = config.clickhouse_user
    ch_password = config.clickhouse_password
    ch_db = config.clickhouse_db

    def write_batch(batch_df: DataFrame, batch_id: int):
        out_df = batch_df.select(
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
            col("song_year"),
            col("artist_location"),
            col("_processing_time"),
        )

        out_df = out_df.fillna({
            "event_id": "", "user_id": 0, "artist": "", "song": "", "duration": 0.0, 
            "session_id": "", "city": "", "state": "",
            "enriched_first_name": "", "enriched_last_name": "", "enriched_gender": "", 
            "enriched_city": "", "enriched_state": "", "enriched_zip": "",
            "song_year": "", "artist_location": ""
        })

        pdf = out_df.toPandas()
        if not pdf.empty:
            import clickhouse_connect
            client = clickhouse_connect.get_client(
                host=ch_host,
                port=ch_port,
                username=ch_user,
                password=ch_password,
                database=ch_db
            )
            client.insert_df("silver_playback_events", pdf)

        logger.info(
            "✓ Batch %d: wrote %d enriched rows to ClickHouse via clickhouse-connect.",
            batch_id,
            len(pdf),
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
        # Map localhost to kafka container name for executor
        bootstrap = self.config.kafka_bootstrap_servers.replace("localhost:9093", "kafka:9092").replace("127.0.0.1:9093", "kafka:9092")
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

    def enrich_user_profiles(self, df: DataFrame) -> DataFrame:
        """Apply executor-side Redis lookup via mapInArrow."""
        # Combine schemas: incoming df schema + enriched fields
        from pyspark.sql.types import StructType
        out_schema = StructType(list(df.schema) + list(ENRICHED_USER_PROFILE_SCHEMA))

        # Map localhost to redis container name for executor
        redis_host = self.config.redis_host.replace("localhost", "redis").replace("127.0.0.1", "redis")
        redis_port = self.config.redis_port

        def _arrow_func(batches):
            yield from _enrich_profiles_partition(batches, redis_host, redis_port)

        return df.mapInArrow(_arrow_func, schema=out_schema)

    def enrich_content_metadata(self, df: DataFrame) -> DataFrame:
        """Demonstrate Broadcast Join pattern for low-cardinality dimension dataset."""
        # Read 2MB static songs catalog via Pandas locally, then ship to Spark Connect
        catalog_path = (
            "/Users/melchior/Developer/opendata-stack-platform/"
            "opendata_stack_platform_sqlmesh/seeds/songs.csv"
        )
        catalog_pdf = pd.read_csv(catalog_path)
        
        # spark.createDataFrame ships the local Pandas DF to the remote Spark cluster
        catalog_df = self.spark.createDataFrame(catalog_pdf)

        # Select and deduplicate required dimensions
        dim_df = catalog_df.select(
            col("artist_name"),
            col("title"),
            col("year").cast("string").alias("song_year"),
            col("artist_location"),
        ).dropDuplicates(["artist_name", "title"])

        logger.info("Applying broadcast join for content metadata on 'artist' & 'song'.")
        return df.join(
            broadcast(dim_df),
            on=[df["artist"] == dim_df["artist_name"], df["song"] == dim_df["title"]],
            how="left",
        ).drop("artist_name", "title")

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
        """writeStream sink → ClickHouse via JDBC."""
        chkpt = f"{self.config.checkpoint_path}/{topic}_clickhouse"
        sink_fn = make_clickhouse_sink(self.config)

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
        base_df = self.declare_transformations(source_df, schema)

        # 3. Joins & Enrichment
        content_enriched_df = self.enrich_content_metadata(base_df)
        enriched_df = self.enrich_user_profiles(content_enriched_df)

        # Dual sinks
        iceberg_q = self.declare_iceberg_sink(base_df, topic)
        clickhouse_q = self.declare_clickhouse_sink(enriched_df, topic)

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
