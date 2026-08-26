"""Streamify - Spark Structured Streaming pipeline."""

import logging

from collections.abc import Iterable, Iterator
from contextlib import contextmanager

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    broadcast,
    coalesce,
    col,
    concat_ws,
    current_timestamp,
    from_json,
    lit,
    pandas_udf,
    sha2,
    to_date,
)
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StringType, StructType

import streamify.logger  # noqa: F401

from streamify.bootstrap import bootstrap_storage
from streamify.defs.resources import (
    ClickHouseResource,
    S3Resource,
    StreamingJobConfig,
    create_clickhouse_resource,
    create_s3_resource,
    create_spark_session,
    get_executor_clickhouse_client,
    get_executor_redis_client,
    get_streaming_config,
)
from streamify.schemas import (
    CLICKHOUSE_NULL_DEFAULTS,
    ENRICHED_USER_PROFILE_SCHEMA,
    PROFILE_FIELDS,
    RAW_SCHEMAS,
)

logger = logging.getLogger(__name__)


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


def enrich_profiles_partition(
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
    * **Resilience** - Redis connection/transport errors are caught gracefully,
      falling back to empty string profile defaults so the stream stays alive.
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
            try:
                with r_client.pipeline(transaction=False) as pipe:
                    for uid in uid_list:
                        pipe.hmget(f"user:{uid}", *PROFILE_FIELDS)
                    results = pipe.execute()
                profiles = [tuple(v or "" for v in res) for res in results]
            except Exception as exc:
                logger.warning(
                    "Redis enrichment failed for %d IDs (%s). Defaulting to empty.",
                    len(uid_list),
                    exc,
                )
                profiles = [tuple("" for _ in PROFILE_FIELDS) for _ in uid_list]

        # One Arrow array per profile field, plus a trailing "" sentinel row
        # standing in for null user IDs. ``take`` then re-aligns every row in
        # the batch back to its original order.
        sentinel_idx = pa.scalar(len(profiles), type=pa.int32())
        profile_arrays = [
            pa.array([row[i] for row in profiles] + [""], type=pa.string())
            for i in range(len(PROFILE_FIELDS))
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
        try:
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
            ).fillna(CLICKHOUSE_NULL_DEFAULTS)

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
        except Exception as exc:
            logger.error(
                "✗ Batch %d: failed to write to ClickHouse: %s",
                batch_id,
                exc,
                exc_info=True,
            )
            raise

    return write_batch


# ---------------------------------------------------------------------------
# Pipeline class
# ---------------------------------------------------------------------------


class StreamifyDeclarativePipeline:
    """Declarative Spark Structured Streaming pipeline for Streamify.

    Manages dual sinks: Iceberg lakehouse (bronze) + ClickHouse fast-path
    (silver / enriched).
    """

    def __init__(
        self,
        spark: SparkSession,
        config: StreamingJobConfig,
        clickhouse: ClickHouseResource,
        s3: S3Resource,
    ) -> None:
        self.spark = spark
        self.config = config
        self.clickhouse = clickhouse
        self.s3 = s3
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
        self,
        source_df: DataFrame,
        schema: StructType,
        topic: str = "listen_events",
    ) -> tuple[DataFrame, DataFrame]:
        """Parse JSON payload with PERMISSIVE mode, splitting into valid & DLQ DFs."""
        parsed_raw = source_df.select(
            col("value").cast("string").alias("_raw_payload"),
            from_json(
                col("value").cast("string"),
                schema,
                options={
                    "mode": "PERMISSIVE",
                    "columnNameOfCorruptRecord": "_corrupt_record",
                },
            ).alias("data"),
            col("partition").alias("_kafka_partition"),
            col("offset").alias("_kafka_offset"),
            col("timestamp").alias("_kafka_timestamp"),
        )

        is_corrupt = (
            col("data._corrupt_record").isNotNull()
            | col("data").isNull()
            | col("data.userId").isNull()
            | col("data.ts").isNull()
        )

        # DLQ DF
        dlq_df = (
            parsed_raw.filter(is_corrupt)
            .select(
                col("_raw_payload").alias("raw_payload"),
                lit("ingestion").alias("error_stage"),
                coalesce(
                    col("data._corrupt_record"),
                    lit("Missing required field(s): userId/ts or unparseable payload"),
                ).alias("error_reason"),
                lit(topic).alias("topic"),
                col("_kafka_partition"),
                col("_kafka_offset"),
                col("_kafka_timestamp"),
                current_timestamp().alias("_processing_time"),
            )
            .withColumn("_processing_date", to_date(col("_processing_time")))
        )

        # valid DF
        valid_parsed = parsed_raw.filter(~is_corrupt).select(
            "data.*",
            "_kafka_partition",
            "_kafka_offset",
            "_kafka_timestamp",
        )

        valid_df = (
            valid_parsed.withColumn(
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
            .withColumn("song", pandas_udf(StringType())(string_decode_vec)(col("song")))
            .withColumn(
                "artist", pandas_udf(StringType())(string_decode_vec)(col("artist"))
            )
            .select(
                "artist",
                "song",
                "duration",
                "ts",
                "auth",
                "level",
                "city",
                "zip",
                "state",
                "userAgent",
                "lon",
                "lat",
                "userId",
                "lastName",
                "firstName",
                "gender",
                "registration",
                "sessionId",
                "itemInSession",
                "event_id",
                "event_ts",
                "event_date",
                "_kafka_partition",
                "_kafka_offset",
                "_kafka_timestamp",
                "_processing_time",
            )
        )

        return valid_df, dlq_df

    def enrich_user_profiles(self, df: DataFrame) -> DataFrame:
        """Apply executor-side Redis lookup via ``mapInArrow``."""
        out_schema = StructType(list(df.schema) + list(ENRICHED_USER_PROFILE_SCHEMA))

        redis_host = self.config.executor_redis_host
        redis_port = self.config.redis_port

        def _arrow_func(
            batches: Iterable[pa.RecordBatch],
        ) -> Iterable[pa.RecordBatch]:
            yield from enrich_profiles_partition(batches, redis_host, redis_port)

        return df.mapInArrow(_arrow_func, schema=out_schema)

    def enrich_content_metadata(self, df: DataFrame) -> DataFrame:
        """Broadcast-join the event stream against the static songs catalog.

        The catalog ``DataFrame`` is materialised from S3 the first time this
        method is called and then cached as ``self._catalog_df`` for the
        lifetime of the pipeline session.
        """
        if self._catalog_df is None:
            self._catalog_df = self.load_songs_catalog()

        logger.info("Applying broadcast join for content metadata on 'artist' & 'song'.")
        return df.join(
            broadcast(self._catalog_df),
            on=[
                df["artist"] == self._catalog_df["artist_name"],
                df["song"] == self._catalog_df["title"],
            ],
            how="left",
        ).drop("artist_name", "title")

    def load_songs_catalog(self) -> DataFrame:
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
                "key": self.s3.aws_access_key_id,
                "secret": self.s3.aws_secret_access_key,
                "client_kwargs": {"endpoint_url": self.s3.endpoint_url},
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
        table = f"bronze_{topic}"
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

    def declare_dlq_sink(
        self,
        dlq_df: DataFrame,
        topic: str,
    ) -> StreamingQuery:
        """Start a writeStream sink targeting the Iceberg DLQ table."""
        table = "dlq_events_ingestion"
        chkpt = f"{self.config.checkpoint_path}/{topic}_dlq"
        trigger_interval = self.config.iceberg_trigger_interval

        logger.info(
            "Declaring DLQ sink → %s (trigger=%s)...",
            table,
            trigger_interval,
        )
        return (
            dlq_df.writeStream.format("iceberg")
            .outputMode("append")
            .trigger(processingTime=trigger_interval)
            .option("checkpointLocation", chkpt)
            .option("fanout-enabled", "true")
            .queryName(f"dlq_{topic}")
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

    def init_pipeline(self, topic: str) -> None:
        """Ensure Iceberg namespaces and bronze/DLQ/ClickHouse tables exist."""
        logger.info(
            "Init pipeline: topic=%s, catalog=%s, namespace=%s",
            topic,
            self.config.catalog,
            self.config.namespace,
        )
        bootstrap_storage(
            spark=self.spark,
            clickhouse=self.clickhouse,
            topics=[topic],
            catalog=self.config.catalog,
            namespace=self.config.namespace,
        )

    # ------------------------------------------------------------------
    # Pipeline orchestration
    # ------------------------------------------------------------------

    def run_topic_stream(self, topic: str = "listen_events") -> None:
        """Launch the streaming pipeline with dual sinks + DLQ sink."""
        if topic not in RAW_SCHEMAS:
            raise ValueError(f"Schema not registered for topic '{topic}'")

        raw_schema = RAW_SCHEMAS[topic]

        # 1. Bootstrap pipeline
        self.init_pipeline(topic)

        # 2. Source
        source_df = self.declare_kafka_source(topic)

        # 3. Transformations (splitting into valid & DLQ)
        base_df, dlq_df = self.declare_transformations(source_df, raw_schema, topic)

        # 4. Enrichment (broadcast join first, then executor-side Redis lookup)
        content_enriched_df = self.enrich_content_metadata(base_df)
        enriched_df = self.enrich_user_profiles(content_enriched_df)

        # 5. Triple sinks
        #    - Iceberg receives raw parsed base_df (bronze)
        #    - ClickHouse receives fully enriched_df (silver)
        #    - Iceberg receives invalid/corrupt records (dlq)
        iceberg_q: StreamingQuery = self.declare_iceberg_sink(base_df, topic)  # type: ignore[assignment]
        clickhouse_q: StreamingQuery = self.declare_clickhouse_sink(enriched_df, topic)  # type: ignore[assignment]
        dlq_q: StreamingQuery = self.declare_dlq_sink(dlq_df, topic)  # type: ignore[assignment]

        logger.info(
            "✓ Streams started: Iceberg=%s, ClickHouse=%s, DLQ=%s.",
            iceberg_q.id,  # type: ignore[union-attr]
            clickhouse_q.id,  # type: ignore[union-attr]
            dlq_q.id,  # type: ignore[union-attr]
        )

        # 6. Lifecycle management via supervisor
        with supervise_streaming_queries([iceberg_q, clickhouse_q, dlq_q]):
            try:
                logger.info("Awaiting termination... Ctrl+C to stop.")
                self.spark.streams.awaitAnyTermination()
            except KeyboardInterrupt:
                logger.info("KeyboardInterrupt received.")


@contextmanager
def supervise_streaming_queries(
    queries: Iterable[StreamingQuery],
) -> Iterator[list[StreamingQuery]]:
    """Context manager ensuring all streaming queries are stopped on termination."""
    active_queries = list(queries)
    try:
        yield active_queries
    finally:
        logger.info("Stopping streaming queries...")
        for q in active_queries:
            try:
                if q.isActive:
                    q.stop()
            except Exception as exc:
                logger.warning(
                    "Error stopping query %s: %s",
                    getattr(q, "id", "unknown"),
                    exc,
                )
        logger.info("✓ Queries stopped cleanly.")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main() -> None:
    """Main entrypoint for the Streamify pipeline."""
    logger.info("Initializing Spark session and config...")
    cfg = get_streaming_config()
    spark = create_spark_session(cfg)
    s3 = create_s3_resource(cfg)
    clickhouse = create_clickhouse_resource(cfg)

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

    pipeline = StreamifyDeclarativePipeline(
        spark=spark,
        config=cfg,
        clickhouse=clickhouse,
        s3=s3,
    )
    pipeline.run_topic_stream("listen_events")


if __name__ == "__main__":
    main()
