"""Streamify - Spark Structured Streaming pipeline."""

import logging

from collections.abc import Iterable, Iterator
from contextlib import contextmanager

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

import streamify.logger  # noqa: F401

from streamify.bootstrap import bootstrap_storage
from streamify.defs.resources import (
    ClickHouseResource,
    StreamingJobConfig,
    create_clickhouse_resource,
    create_s3_resource,
    create_spark_session,
    get_streaming_config,
)
from streamify.resources import (
    ClickHouseSink,
    IcebergSink,
    KafkaSource,
    RedisProfileEnricher,
    SongsMetadataEnricher,
    StreamingSink,
    StreamingSource,
    StreamTransformer,
)
from streamify.schemas import RAW_SCHEMAS
from streamify.transformations import parse_raw_events_with_dlq

logger = logging.getLogger(__name__)


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


class StreamifyDeclarativePipeline:
    """Declarative Spark Structured Streaming pipeline for Streamify.

    Manages dual sinks: Iceberg lakehouse (bronze) + ClickHouse fast-path
    (silver / enriched) + Iceberg DLQ table.
    """

    def __init__(  # noqa: PLR0913, PLR0917
        self,
        spark: SparkSession,
        config: StreamingJobConfig,
        source: StreamingSource,
        songs_enricher: StreamTransformer,
        redis_enricher: StreamTransformer,
        bronze_sink: StreamingSink,
        clickhouse_sink: StreamingSink,
        dlq_sink: StreamingSink,
        clickhouse: ClickHouseResource,
    ) -> None:
        self.spark = spark
        self.config = config
        self.source = source
        self.songs_enricher = songs_enricher
        self.redis_enricher = redis_enricher
        self.bronze_sink = bronze_sink
        self.clickhouse_sink = clickhouse_sink
        self.dlq_sink = dlq_sink
        self.clickhouse = clickhouse

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

    def run_topic_stream(self, topic: str = "listen_events") -> None:
        """Launch the streaming pipeline with dual sinks + DLQ sink."""
        if topic not in RAW_SCHEMAS:
            raise ValueError(f"Schema not registered for topic '{topic}'")

        raw_schema = RAW_SCHEMAS[topic]

        # 1. Bootstrap storage/catalog
        self.init_pipeline(topic)

        # 2. Ingest from Source Strategy
        source_df = self.source.read(self.spark)

        # 3. Transformations (splitting into valid & DLQ)
        base_df, dlq_df = parse_raw_events_with_dlq(source_df, raw_schema, topic)

        # 4. Enrichments (broadcast join first, then executor-side Redis lookup)
        content_enriched_df = self.songs_enricher.transform(base_df)
        enriched_df = self.redis_enricher.transform(content_enriched_df)

        # 5. Triple sinks
        #    - Iceberg receives raw parsed base_df (bronze)
        #    - ClickHouse receives fully enriched_df (silver)
        #    - Iceberg receives invalid/corrupt records (dlq)
        iceberg_q: StreamingQuery = self.bronze_sink.write(base_df, topic)
        clickhouse_q: StreamingQuery = self.clickhouse_sink.write(enriched_df, topic)
        dlq_q: StreamingQuery = self.dlq_sink.write(dlq_df, topic)

        logger.info(
            "✓ Streams started: Iceberg=%s, ClickHouse=%s, DLQ=%s.",
            iceberg_q.id,
            clickhouse_q.id,
            dlq_q.id,
        )

        # 6. Lifecycle management via supervisor
        with supervise_streaming_queries([iceberg_q, clickhouse_q, dlq_q]):
            try:
                logger.info("Awaiting termination... Ctrl+C to stop.")
                self.spark.streams.awaitAnyTermination()
            except KeyboardInterrupt:
                logger.info("KeyboardInterrupt received.")


def main() -> None:
    """Main entrypoint for the Streamify pipeline (Composition Root)."""
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

    topic = "listen_events"

    # 1. Source Strategy
    source = KafkaSource(
        bootstrap_servers=cfg.executor_kafka_bootstrap_servers,
        topic=topic,
        max_offsets=cfg.max_offsets_per_trigger,
    )

    # 2. Transformer Strategies (Enrichers)
    songs_enricher = SongsMetadataEnricher(
        s3=s3,
        catalog_path=cfg.songs_catalog_path,
        spark=spark,
    )
    redis_enricher = RedisProfileEnricher(
        host=cfg.executor_redis_host,
        port=cfg.redis_port,
    )

    # 3. Sink Strategies
    bronze_sink = IcebergSink(
        chkpt=f"{cfg.checkpoint_path}/{topic}",
        query_name=f"bronze_{topic}",
        table_name=f"bronze_{topic}",
        trigger_interval=cfg.iceberg_trigger_interval,
    )
    clickhouse_sink = ClickHouseSink(
        resource=clickhouse,
        table_name="silver_playback_events",
        checkpoint_path=cfg.checkpoint_path,
        topic=topic,
        trigger_interval=cfg.clickhouse_trigger_interval,
    )
    dlq_sink = IcebergSink(
        chkpt=f"{cfg.checkpoint_path}/{topic}_dlq",
        query_name=f"dlq_{topic}",
        table_name="dlq_events_ingestion",
        trigger_interval=cfg.iceberg_trigger_interval,
    )

    # 4. Assemble and run orchestrator
    pipeline = StreamifyDeclarativePipeline(
        spark=spark,
        config=cfg,
        source=source,
        songs_enricher=songs_enricher,
        redis_enricher=redis_enricher,
        bronze_sink=bronze_sink,
        clickhouse_sink=clickhouse_sink,
        dlq_sink=dlq_sink,
        clickhouse=clickhouse,
    )
    pipeline.run_topic_stream(topic)


if __name__ == "__main__":
    main()
