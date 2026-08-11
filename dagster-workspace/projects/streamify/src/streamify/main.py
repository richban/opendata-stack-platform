"""Streamify - Spark Structured Streaming pipeline."""

import logging
import sys

from collections.abc import Iterable, Iterator

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    broadcast,
    col,
    concat_ws,
    current_timestamp,
    from_json,
    pandas_udf,
    sha2,
    to_date,
)
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StringType, StructType

from streamify.defs.bronze_assets import (
    create_namespace_if_not_exists,
    create_table_if_not_exists,
)
from streamify.clients import (
    get_executor_clickhouse_client,
    get_executor_redis_client,
)
from streamify.defs.resources import (
    StreamingJobConfig,
    create_clickhouse_resource,
    create_spark_session,
    get_streaming_config,
)
from streamify.log import configure_logging
from streamify.schemas import (
    ENRICHED_USER_PROFILE_SCHEMA,
    SCHEMAS as TOPIC_SCHEMAS,
    meta_schema,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
configure_logging()
logger = logging.getLogger("streamify.main")


# ---------------------------------------------------------------------------
# Vectorised string decode
# ---------------------------------------------------------------------------


def _string_decode_fn(s: str, encoding: str = "utf-8") -> str:
    """Decode unicode/octal-escaped strings (e.g. artist/song names from eventsim)."""
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


def string_decode_vec(series: pd.Series) -> pd.Series:  # type: ignore[type-arg]
    """Vectorised wrapper around ``_string_decode_fn``."""
    return series.apply(_string_decode_fn)  # type: ignore[return-value]  # ty: ignore[invalid-return-type]


# ---------------------------------------------------------------------------
# ClickHouse DDL bootstrap
# ---------------------------------------------------------------------------


def ensure_clickhouse_table_exists(config: StreamingJobConfig) -> None:
    """Create ClickHouse database and ``ReplacingMergeTree`` table if absent."""
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


# ---------------------------------------------------------------------------
# ClickHouse null defaults (single source of truth)
# ---------------------------------------------------------------------------

_CLICKHOUSE_NULL_DEFAULTS: dict[str, int | float | str] = {
    "event_id": "",
    "user_id": 0,
    "artist": "",
    "song": "",
    "duration": 0.0,
    "session_id": "",
    "city": "",
    "state": "",
    "enriched_first_name": "",
    "enriched_last_name": "",
    "enriched_gender": "",
    "enriched_city": "",
    "enriched_state": "",
    "enriched_zip": "",
    "song_year": "",
    "artist_location": "",
}

# Ordered list of columns written to ClickHouse - derived from the table DDL
# rather than repeated inline in write_batch.
_CLICKHOUSE_COLUMNS: list[str] = [
    "event_id",
    "user_id",
    "artist",
    "song",
    "duration",
    "event_ts",
    "session_id",
    "city",
    "state",
    "enriched_first_name",
    "enriched_last_name",
    "enriched_gender",
    "enriched_city",
    "enriched_state",
    "enriched_zip",
    "song_year",
    "artist_location",
    "_processing_time",
]


# ---------------------------------------------------------------------------
# Executor-side mapInArrow: Redis profile enrichment
# ---------------------------------------------------------------------------


# Field names fetched from each ``user:<id>`` Redis hash, ordered to match
# ``ENRICHED_USER_PROFILE_SCHEMA``.
_PROFILE_FIELDS: tuple[str, ...] = (
    "first_name",
    "last_name",
    "gender",
    "city",
    "state",
    "zip_code",
)


def _enrich_profiles_partition(
    batches: Iterable[pa.RecordBatch],
    redis_host: str,
    redis_port: int,
) -> Iterator[pa.RecordBatch]:
    """PyArrow partition iterator for executor-side Redis lookups.

    Design
    ------
    * **Executor-side** - Redis I/O happens on distributed worker nodes, not
      the driver JVM.
    * **Arrow-native alignment** - dedup, re-ordering, and column assembly all
      happen inside ``pyarrow.compute``.  The only data that round-trips through
      Python is the *set of distinct user IDs* needed to build Redis keys; the
      fetched profiles are re-aligned to the original row order with
      ``index_in``/``take`` instead of a per-row Python dict loop.
    * **Per-batch dedup + pipeline** - unique user IDs within each Arrow batch
      are collected, then fetched in a *single* pipelined Redis round-trip.
      There is intentionally no cross-batch in-memory cache: at Spotify/Netflix
      scale (300 M+ users) an unbounded executor-side cache creates severe
      memory pressure.  Redis is designed to serve millions of ops/sec; let it
      do its job.
    """
    r_client = get_executor_redis_client(redis_host, redis_port)
    enriched_fields = ENRICHED_USER_PROFILE_SCHEMA.fieldNames()

    for batch in batches:
        if batch.num_rows == 0:
            yield batch
            continue

        uid_col = batch.column("userId")
        unique_ids = pc.drop_null(pc.unique(uid_col))  # ty: ignore[unresolved-attribute]
        uid_list = unique_ids.to_pylist()

        profiles: list[tuple[str, ...]] = []
        if uid_list:
            with r_client.pipeline(transaction=False) as pipe:
                for uid in uid_list:
                    pipe.hmget(f"user:{uid}", *_PROFILE_FIELDS)
                results = pipe.execute()
            profiles = [tuple(v or "" for v in res) for res in results]

        # One Arrow array per profile field, plus a trailing "" sentinel row
        # standing in for null user IDs. ``take`` then re-aligns every row in
        # the batch back to its original order.
        sentinel_idx = pa.scalar(len(profiles), type=pa.int32())
        profile_arrays = [
            pa.array([row[i] for row in profiles] + [""], type=pa.string())
            for i in range(len(_PROFILE_FIELDS))
        ]
        positions = pc.fill_null(
            pc.index_in(  # ty: ignore[unresolved-attribute]
                uid_col, unique_ids, skip_nulls=True
            ),
            sentinel_idx,
        )
        aligned_arrays = [col.take(positions) for col in profile_arrays]

        new_arrays = [*batch.columns, *aligned_arrays]
        new_names = [*batch.schema.names, *enriched_fields]
        yield pa.RecordBatch.from_arrays(new_arrays, names=new_names)


def make_clickhouse_sink(config: StreamingJobConfig):
    """Return a ``foreachBatch`` handler that writes enriched rows to ClickHouse."""
    ch_host = config.executor_clickhouse_host
    ch_port = config.clickhouse_port
    ch_user = config.clickhouse_user
    ch_password = config.clickhouse_password
    ch_db = config.clickhouse_db

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
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
        ).fillna(_CLICKHOUSE_NULL_DEFAULTS)

        arrow_table = out_df.toArrow()
        client = get_executor_clickhouse_client(
            ch_host, ch_port, ch_user, ch_password, ch_db
        )
        client.insert_arrow("silver_playback_events", arrow_table)
        logger.info(
            "✓ Batch %d: wrote %d enriched rows to ClickHouse.",
            batch_id,
            arrow_table.num_rows,
        )

    return write_batch


# ---------------------------------------------------------------------------
# Pipeline class
# ---------------------------------------------------------------------------


class StreamifyDeclarativePipeline:
    """Declarative Spark Structured Streaming pipeline for Streamify.

    Manages dual sinks: Iceberg lakehouse (bronze) + ClickHouse fast-path
    (silver / enriched).
    """

    def __init__(self, spark: SparkSession, config: StreamingJobConfig) -> None:
        self.spark = spark
        self.config = config
        # Lazy-loaded catalog DataFrame - materialised once per session.
        self._catalog_df: DataFrame | None = None

    # ------------------------------------------------------------------
    # Source
    # ------------------------------------------------------------------

    def declare_kafka_source(self, topic: str) -> DataFrame:
        """Return a streaming ``DataFrame`` reading from the given Kafka topic."""
        bootstrap = self.config.executor_kafka_bootstrap_servers
        logger.info(
            "Declaring Kafka source for '%s' (bootstrap=%s)...",
            topic,
            bootstrap,
        )
        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", self.config.max_offsets_per_trigger)
            .load()
        )

    # ------------------------------------------------------------------
    # Transformations
    # ------------------------------------------------------------------

    def declare_transformations(
        self, source_df: DataFrame, schema: StructType
    ) -> DataFrame:
        """Parse JSON payload, generate ``event_id``, and compute ``event_ts``."""
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
            .withColumn("event_ts", (col("ts") / 1000).cast("timestamp"))
            .withColumn("event_date", to_date(col("event_ts")))
            .withColumn("_processing_time", current_timestamp())
            # Vectorised pandas_udf — no per-row Python call overhead
            .withColumn("song", pandas_udf(StringType())(string_decode_vec)(col("song")))
            .withColumn(
                "artist", pandas_udf(StringType())(string_decode_vec)(col("artist"))
            )
        )

        metadata_cols = [f.name for f in meta_schema]
        data_cols = [f.name for f in schema.fields]
        return parsed_df.select(*data_cols, *metadata_cols)

    def enrich_user_profiles(self, df: DataFrame) -> DataFrame:
        """Apply executor-side Redis lookup via ``mapInArrow``."""
        out_schema = StructType(list(df.schema) + list(ENRICHED_USER_PROFILE_SCHEMA))

        redis_host = self.config.executor_redis_host
        redis_port = self.config.redis_port

        def _arrow_func(
            batches: Iterable[pa.RecordBatch],
        ) -> Iterable[pa.RecordBatch]:
            yield from _enrich_profiles_partition(batches, redis_host, redis_port)

        return df.mapInArrow(_arrow_func, schema=out_schema)

    def enrich_content_metadata(self, df: DataFrame) -> DataFrame:
        """Broadcast-join the event stream against the static songs catalog.

        The catalog ``DataFrame`` is materialised from S3 the first time this
        method is called and then cached as ``self._catalog_df`` for the
        lifetime of the pipeline session.
        """
        if self._catalog_df is None:
            self._catalog_df = self._load_songs_catalog()

        logger.info("Applying broadcast join for content metadata on 'artist' & 'song'.")
        return df.join(
            broadcast(self._catalog_df),
            on=[
                df["artist"] == self._catalog_df["artist_name"],
                df["song"] == self._catalog_df["title"],
            ],
            how="left",
        ).drop("artist_name", "title")

    def _load_songs_catalog(self) -> DataFrame:
        """Load the songs catalog from S3 and return a deduplicated dim DataFrame.

        The catalog path is driven by ``config.songs_catalog_path``
        (default: ``s3a://datalake/seeds/songs.csv``).
        """
        path = self.config.songs_catalog_path
        logger.info("Loading songs catalog from '%s'...", path)

        # Read via Pandas using fsspec/s3fs so we stay in the driver process
        # and then ship to Spark Connect.  The CSV is small (~2 MB) so this is
        # fine; for larger catalogs consider reading via spark.read.csv directly.
        catalog_pdf = pd.read_csv(
            path,
            storage_options={
                "key": self.config.aws_access_key_id,
                "secret": self.config.aws_secret_access_key,
                "client_kwargs": {"endpoint_url": self.config.aws_endpoint_url},
            },
        )
        catalog_df = self.spark.createDataFrame(catalog_pdf)

        dim_df = (
            catalog_df.select(
                col("artist_name"),
                col("title"),
                col("year").cast("string").alias("song_year"),
                col("artist_location"),
            )
            .dropDuplicates(["artist_name", "title"])
            .cache()
        )

        # Force materialization of the cache immediately
        num_rows = dim_df.count()
        logger.info("✓ Songs catalog loaded (%d rows after dedup).", num_rows)
        return dim_df

    # ------------------------------------------------------------------
    # Sinks
    # ------------------------------------------------------------------

    def declare_iceberg_sink(
        self,
        transformed_df: DataFrame,
        topic: str,
    ) -> StreamingQuery:
        """Start a writeStream sink targeting the Iceberg lakehouse (bronze layer)."""
        table = f"{self.config.catalog}.{self.config.namespace}.bronze_{topic}"
        chkpt = f"{self.config.checkpoint_path}/{topic}"
        trigger_interval = self.config.iceberg_trigger_interval

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
    ) -> StreamingQuery:
        """Start a writeStream sink targeting ClickHouse (silver layer)."""
        chkpt = f"{self.config.checkpoint_path}/{topic}_clickhouse"
        sink_fn = make_clickhouse_sink(self.config)
        trigger_interval = self.config.clickhouse_trigger_interval

        logger.info(
            "Declaring ClickHouse sink → %s (trigger=%s)...",
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

    # ------------------------------------------------------------------
    # Schema bootstrapping
    # ------------------------------------------------------------------

    def ensure_target_schema(self, topic: str, schema: StructType) -> None:
        """Ensure the Iceberg namespace and bronze table exist before streaming."""
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

    # ------------------------------------------------------------------
    # Pipeline orchestration
    # ------------------------------------------------------------------

    def run_topic_stream(self, topic: str = "listen_events") -> None:
        """Launch the dual-sink streaming pipeline for *topic*."""
        if topic not in TOPIC_SCHEMAS:
            raise ValueError(f"Schema not registered for topic '{topic}'")

        schema = TOPIC_SCHEMAS[topic]

        # 1. Target schema preparation
        self.ensure_target_schema(topic, schema)
        ensure_clickhouse_table_exists(self.config)

        # 2. Source
        source_df = self.declare_kafka_source(topic)

        # 3. Transformations
        base_df = self.declare_transformations(source_df, schema)

        # 4. Enrichment (broadcast join first, then executor-side Redis lookup)
        content_enriched_df = self.enrich_content_metadata(base_df)
        enriched_df = self.enrich_user_profiles(content_enriched_df)

        # 5. Dual sinks
        #    - Iceberg receives the raw parsed base_df (bronze)
        #    - ClickHouse receives the fully enriched_df (silver)
        iceberg_q: StreamingQuery = self.declare_iceberg_sink(base_df, topic)  # type: ignore[assignment]
        clickhouse_q: StreamingQuery = self.declare_clickhouse_sink(enriched_df, topic)  # type: ignore[assignment]

        logger.info(
            "✓ Streams started: Iceberg=%s, ClickHouse=%s.",
            iceberg_q.id,  # type: ignore[union-attr]
            clickhouse_q.id,  # type: ignore[union-attr]
        )

        # 6. Lifecycle management
        try:
            logger.info("Awaiting termination... Ctrl+C to stop.")
            self.spark.streams.awaitAnyTermination()
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received.")
        finally:
            logger.info("Stopping streaming queries...")
            iceberg_q.stop()  # type: ignore[union-attr]
            clickhouse_q.stop()  # type: ignore[union-attr]
            logger.info("✓ Queries stopped cleanly.")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main() -> None:
    """Main entrypoint for the Streamify pipeline."""
    logger.info("Initializing Spark session and config...")
    spark = create_spark_session()
    cfg = get_streaming_config()

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
