"""Streamify - Spark Structured Streaming pipeline."""

import logging

from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from typing import Protocol, List

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

from streamify.resources import StreamingSink, StreamingSource, RedisProfileEnricher

from streamify.transformations import enrich_profiles_partition, parse_raw_events_with_dlq

logger = logging.getLogger(__name__)


class StreamifyDeclarativePipeline:
    """Declarative Spark Structured Streaming pipeline for Streamify.

    Manages dual sinks: Iceberg lakehouse (bronze) + ClickHouse fast-path
    (silver / enriched).
    """

    def __init__(
        self,
        spark: SparkSession,
        config: StreamingJobConfig,
        source: StreamingSource,
        clickhouse: ClickHouseResource,
        redis_enricher: RedisProfileEnricher,
        s3: S3Resource,
        clickhouse_sink: StreamingSink,
        iceberg_sink: StreamingSink,
        dlq_sink: StreStreamingSink,
    ) -> None:
        self.spark = spark
        self.config = config
        self.source = source
        self.clickhouse = clickhouse
        self.redis_enricher = redis_enricher
        self.s3 = s3
        self.clickhouse_sink = clickhouse_sink
        self.iceberg_sink = iceberg_sink
        self.dlq_sink = dlq_sink
        # Lazy-loaded catalog DataFrame - materialised once per session.
        self._catalog_df: DataFrame | None = None

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

    def run_stream(self, topic: str = "listen_events") -> None:
        """Launch the streaming pipeline with dual sinks + DLQ sink."""
        if topic not in RAW_SCHEMAS:
            raise ValueError(f"Schema not registered for topic '{topic}'")

        raw_schema = RAW_SCHEMAS[topic]

        # 1. Bootstrap pipeline
        bootstrap_storage(
            spark=self.spark,
            clickhouse=self.clickhouse,
            topics=[topic],
            catalog=self.config.catalog,
            namespace=self.config.namespace,
        )

        # 2. Ingest from  Source
        source_df = self.source.read(self.spark)

        # 3. Parse & Split DLQ
        base_df, dlq_df = parse_raw_events_with_dlq(source_df, raw_schema, topic)

        # 4. Enrichments (S3 broadcast + Redis Strategy)
        content_enriched_df = self.enrich_content_metadata(base_df)
        enriched_df = self.redis_enricher.transform(content_enriched_df)

        # 5. Triple sinks
        iceberg_q: StreamingQuery = self.iceberg_sink.write(base_df)
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
