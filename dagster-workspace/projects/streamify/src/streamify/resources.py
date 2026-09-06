import logging

from collections.abc import Iterable
from dataclasses import dataclass
from functools import cache
from typing import Any, Protocol

import clickhouse_connect
import pyarrow as pa
import redis

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import broadcast, col
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType

import streamify.logger  # noqa: F401

from streamify.schemas import ENRICHED_USER_PROFILE_SCHEMA
from streamify.transformations import (
    enrich_profiles_partition,
    project_playback_events_for_clickhouse,
    read_kafka_stream,
    write_iceberg_stream,
)

logger = logging.getLogger(__name__)


@cache
def get_executor_redis_client(host: str, port: int) -> redis.Redis:  # type: ignore[type-arg]
    """Return a cached Redis client for executor use.

    ``@cache`` ensures a single client instance is reused across micro-batches
    in the same Python worker process without reconnecting.
    """
    return redis.Redis(host=host, port=port, decode_responses=True)



@cache
def get_executor_clickhouse_client(
    host: str,
    port: int,
    username: str,
    password: str,
    database: str,
) -> clickhouse_connect.driver.Client:
    """Return a cached clickhouse-connect client for executor use.

    Keyed on connection parameters so the client is reused across micro-batches
    in the same Python worker process without re-establishing connections.
    """
    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
    )


@dataclass(frozen=True)
class RedisStreamingResource:
    """Standalone streaming Redis resource for Spark executors."""

    host: str = "localhost"
    port: int = 6379

    def get_client(self) -> redis.Redis:  # type: ignore[type-arg]
        return get_executor_redis_client(host=self.host, port=self.port)


@dataclass(frozen=True)
class ClickHouseStreamingResource:
    """Standalone streaming ClickHouse resource for Spark executors."""

    host: str = "localhost"
    port: int = 8123
    username: str = "default"
    password: str = "clickhouse"
    database: str = "streamify"

    def get_client(self) -> clickhouse_connect.driver.Client:
        return get_executor_clickhouse_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
        )


class StreamingSource(Protocol):
    def read(self, spark: SparkSession) -> DataFrame: ...


class StreamingSink(Protocol):
    def write(self, df: DataFrame, topic: str | None = None) -> StreamingQuery: ...


class StreamTransformer(Protocol):
    def transform(self, df: DataFrame) -> DataFrame: ...


class StreamingIOManager(StreamingSource, StreamingSink, Protocol):
    """If a storage engine can do both source/sink."""

    pass


class KafkaSource:
    """Streaming source strategy for reading Kafka topics."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        max_offsets: int = 10_000,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.max_offsets = max_offsets

    def read(self, spark: SparkSession) -> DataFrame:
        logger.info(
            "Reading Kafka stream: topic='%s', bootstrap='%s'...",
            self.topic,
            self.bootstrap_servers,
        )
        return read_kafka_stream(
            spark, self.bootstrap_servers, self.topic, self.max_offsets
        )


class SongsMetadataEnricher:
    """Strategy for static S3 songs catalog broadcast enrichment."""

    def __init__(
        self,
        spark: SparkSession,
        catalog_path: str,
    ) -> None:
        self.spark = spark
        self.catalog_path = catalog_path
        self._catalog_df: DataFrame | None = None

    def _load_catalog(self) -> DataFrame:
        logger.info("Loading songs catalog from '%s' via Spark...", self.catalog_path)
        dim_df = (
            self.spark.read.option("header", "true")
            .csv(self.catalog_path)
            .select(
                col("artist_name"),
                col("title"),
                col("year").cast("string").alias("song_year"),
                col("artist_location"),
            )
            .dropDuplicates(["artist_name", "title"])
            .cache()
        )
        num_rows = dim_df.count()
        logger.info("✓ Songs catalog loaded and cached (%d rows after dedup).", num_rows)
        return dim_df

    def transform(self, df: DataFrame) -> DataFrame:
        if self._catalog_df is None:
            self._catalog_df = self._load_catalog()

        logger.info("Applying broadcast join for content metadata on 'artist' & 'song'.")
        return df.join(
            broadcast(self._catalog_df),
            on=[
                df["artist"] == self._catalog_df["artist_name"],
                df["song"] == self._catalog_df["title"],
            ],
            how="left",
        ).drop("artist_name", "title")


class RedisProfileEnricher:
    """Executor-side Redis user profile enrichment using PyArrow mapInArrow."""

    def __init__(
        self,
        resource: RedisStreamingResource | None = None,
        host: str | None = None,
        port: int | None = None,
    ) -> None:
        if resource is not None:
            self.resource = resource
        else:
            self.resource = RedisStreamingResource(
                host=host or "localhost",
                port=port or 6379,
            )

    @property
    def host(self) -> str:
        return self.resource.host

    @property
    def port(self) -> int:
        return self.resource.port

    @classmethod
    def from_resource(cls, resource: Any) -> "RedisProfileEnricher":
        streaming_resource = RedisStreamingResource(
            host=resource.host,
            port=resource.port,
        )
        return cls(resource=streaming_resource)

    def transform(self, df: DataFrame) -> DataFrame:
        """Apply executor-side Redis lookup (enrichment) via mapInArrow."""
        out_schema = StructType(list(df.schema) + list(ENRICHED_USER_PROFILE_SCHEMA))

        def _arrow_partition_func(
            batches: Iterable[pa.RecordBatch],
        ) -> Iterable[pa.RecordBatch]:
            yield from enrich_profiles_partition(
                batches=batches,
                redis_host=self.host,
                redis_port=self.port,
            )

        return df.mapInArrow(_arrow_partition_func, schema=out_schema)


class ClickHouseSink:
    """Streaming sink strategy for writing micro-batches to ClickHouse."""

    def __init__(
        self,
        resource: ClickHouseStreamingResource,
        table_name: str,
        checkpoint_path: str,
        topic: str = "listen_events",
        trigger_interval: str = "10 seconds",
    ) -> None:
        self.resource = resource
        self.table_name = table_name
        self.checkpoint_path = checkpoint_path
        self.topic = topic
        self.trigger_interval = trigger_interval

    @property
    def client(self) -> clickhouse_connect.driver.Client:
        """Lazily initialize and reuse the ClickHouse client from injected resource."""
        return self.resource.get_client()

    def write_batch(self, df: DataFrame, batch_id: int) -> None:
        """ForeachBatch handler dispatching parallel partition writes to executors."""
        try:
            projected_df = project_playback_events_for_clickhouse(df)
            resource = self.resource
            table_name = self.table_name
            columns = list(projected_df.columns)

            def _write_partition(rows: Iterable[Any]) -> None:
                part_logger = logging.getLogger("streamify.executor.clickhouse")
                client = resource.get_client()
                data = [tuple(row) for row in rows]
                if data:
                    client.insert(
                        table=table_name,
                        data=data,
                        column_names=columns,
                    )
                    part_logger.info(
                        "Batch %d: worker inserted %d rows into '%s'.",
                        batch_id,
                        len(data),
                        table_name,
                    )

            projected_df.foreachPartition(_write_partition)
            logger.info(
                "✓ Batch %d: distributed parallel insert completed to '%s'.",
                batch_id,
                self.table_name,
            )
        except Exception as exc:
            logger.error(
                "✗ Batch %d: failed parallel write to ClickHouse table '%s': %s",
                batch_id,
                self.table_name,
                exc,
                exc_info=True,
            )
            raise

    def write(self, df: DataFrame, topic: str | None = None) -> StreamingQuery:
        """Start the Structured Streaming query."""
        topic_name = topic or self.topic
        chkpt = f"{self.checkpoint_path}/{topic_name}_clickhouse"
        logger.info(
            "Declaring ClickHouse sink → table=%s, checkpoint=%s (trigger=%s)...",
            self.table_name,
            chkpt,
            self.trigger_interval,
        )
        return (
            df.writeStream.trigger(processingTime=self.trigger_interval)
            .option("checkpointLocation", chkpt)
            .queryName(f"clickhouse_{topic_name}")
            .foreachBatch(self.write_batch)
            .start()
        )


class IcebergSink:
    """Sink for writing to Iceberg tables."""

    def __init__(
        self,
        chkpt: str,
        query_name: str,
        table_name: str,
        trigger_interval: str = "30 seconds",
    ) -> None:
        self.chkpt = chkpt
        self.query_name = query_name
        self.table_name = table_name
        self.trigger_interval = trigger_interval

    def write(self, df: DataFrame, topic: str | None = None) -> StreamingQuery:
        """Start the Iceberg Structured Streaming writeStream."""
        logger.info(
            "Declaring Iceberg sink → table=%s, checkpoint=%s (trigger=%s)...",
            self.table_name,
            self.chkpt,
            self.trigger_interval,
        )
        return write_iceberg_stream(
            df=df,
            chkpt=self.chkpt,
            query_name=self.query_name,
            table_name=self.table_name,
            trigger_interval=self.trigger_interval,
        )
