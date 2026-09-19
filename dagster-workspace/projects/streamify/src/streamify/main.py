"""Streamify - Spark Structured Streaming pipeline."""

import logging

from collections.abc import Iterable, Iterator
from contextlib import contextmanager

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.streaming import StreamingQuery

import streamify.logger  # noqa: F401

from streamify.bootstrap import bootstrap_storage
from streamify.constants import CLICKHOUSE_PLAYBACK_EVENTS_TABLE, DLQ_TABLE
from streamify.defs.resources import (
    ClickHouseResource,
    StreamingJobConfig,
    create_clickhouse_resource,
    create_spark_session,
    get_streaming_config,
)
from streamify.resources import (
    ClickHouseSink,
    ClickHouseStreamingResource,
    IcebergSink,
    KafkaSource,
    RedisProfileEnricher,
    RedisStreamingResource,
    SongsMetadataEnricher,
    StreamingSink,
    StreamingSource,
    StreamTransformer,
)
from streamify.schema_registry import StreamSchemaConfig
from streamify.schemas import RAW_SCHEMAS
from streamify.transformations.events import (
    add_event_metadata,
    decode_raw_events,
    project_bronze_events,
    route_corrupt_records,
)
from streamify.wire import WireFormat

logger = logging.getLogger(__name__)


SCHEMA_CONFIG = StreamSchemaConfig(
    wire_format_by_topic={
        "listen_events": WireFormat.JSON,
        "user_profiles": WireFormat.AVRO,
    },
)


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
    """Streaming (speed) layer: Kafka -> bronze (raw) + ClickHouse + DLQ.

    The batch layer owns silver and quarantine (see
    ``streamify.defs.silver_assets``). Unparseable payloads are dead-lettered
    here, at the point of decode.
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
        schema_config: StreamSchemaConfig | None = None,
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
        self.schema_config = schema_config or StreamSchemaConfig()

    def wire_format_for(self, topic: str) -> WireFormat:
        return self.schema_config.wire_format_for(topic)

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
        """Launch the speed layer: bronze (raw) + ClickHouse + DLQ."""
        if topic not in RAW_SCHEMAS:
            raise ValueError(f"Schema not registered for topic '{topic}'")

        wire_format = self.wire_format_for(topic)

        # 1. Bootstrap storage/catalog
        self.init_pipeline(topic)

        # 2. Ingest from Source Strategy
        source_df = self.source.read(self.spark)

        # 3. Bronze: every record, untouched, with its resolved schema id
        bronze_df = project_bronze_events(source_df, wire_format)

        queries: list[StreamingQuery] = [self.bronze_sink.write(bronze_df, topic)]

        if wire_format is WireFormat.JSON:
            # Decode PERMISSIVE: unparseable payloads land in ``_corrupt_record``
            # and are dead-lettered; the rest are enriched for the fast path.
            decoded_df = decode_raw_events(bronze_df, RAW_SCHEMAS[topic])
            clean_df, corrupt_dlq_df = route_corrupt_records(decoded_df, topic)

            typed_df = add_event_metadata(clean_df).filter(
                col("userId").isNotNull() & col("ts").isNotNull()
            )
            content_enriched_df = self.songs_enricher.transform(typed_df)
            enriched_df = self.redis_enricher.transform(content_enriched_df)

            queries.extend(
                [
                    self.clickhouse_sink.write(enriched_df, topic),
                    self.dlq_sink.write(corrupt_dlq_df, topic),
                ]
            )

        logger.info(
            "✓ Streams started: sinks=%d, topic=%s, wire_format=%s.",
            len(queries),
            topic,
            wire_format,
        )

        # 5. Lifecycle management via supervisor
        with supervise_streaming_queries(queries):
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

    # 2. Transformers (Enrichers)
    songs_enricher = SongsMetadataEnricher(
        spark=spark,
        catalog_path=cfg.songs_catalog_path,
    )
    redis_resource = RedisStreamingResource(
        host=cfg.executor_redis_host,
        port=cfg.redis_port,
    )
    redis_enricher = RedisProfileEnricher(resource=redis_resource)

    # 3. Sinks (speed layer: bronze + ClickHouse + DLQ)
    bronze_sink = IcebergSink(
        chkpt=f"{cfg.checkpoint_path}/{topic}_bronze",
        query_name=f"bronze_{topic}",
        table_name=f"bronze_{topic}",
        trigger_interval=cfg.iceberg_trigger_interval,
    )
    dlq_sink = IcebergSink(
        chkpt=f"{cfg.checkpoint_path}/{topic}_dlq",
        query_name=f"dlq_{topic}",
        table_name=DLQ_TABLE,
        trigger_interval=cfg.iceberg_trigger_interval,
    )
    clickhouse_resource = ClickHouseStreamingResource(
        host=cfg.executor_clickhouse_host,
        port=cfg.clickhouse_port,
        username=cfg.clickhouse_user,
        password=cfg.clickhouse_password,
        database=cfg.clickhouse_db,
    )
    clickhouse_sink = ClickHouseSink(
        resource=clickhouse_resource,
        table_name=CLICKHOUSE_PLAYBACK_EVENTS_TABLE,
        checkpoint_path=cfg.checkpoint_path,
        topic=topic,
        trigger_interval=cfg.clickhouse_trigger_interval,
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
        schema_config=SCHEMA_CONFIG,
    )
    pipeline.run_topic_stream(topic)


if __name__ == "__main__":
    main()
