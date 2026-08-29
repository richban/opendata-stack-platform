import logging

from collections.abc import Iterable
from typing import Protocol

import clickhouse_connect
import pandas as pd
import pyarrow as pa

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import broadcast, col
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType

from streamify.defs.resources import ClickHouseResource, RedisResource, S3Resource
from streamify.schemas import ENRICHED_USER_PROFILE_SCHEMA
from streamify.transformations import (
    enrich_profiles_partition,
    project_playback_events_for_clickhouse,
    read_kafka_stream,
    write_iceberg_stream,
)

logger = logging.getLogger(__name__)


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
        s3: S3Resource,
        catalog_path: str,
        spark: SparkSession,
    ) -> None:
        self.s3 = s3
        self.catalog_path = catalog_path
        self.spark = spark
        self._catalog_df: DataFrame | None = None

    def _load_catalog(self) -> DataFrame:
        logger.info("Loading songs catalog from '%s'...", self.catalog_path)
        catalog_pdf = pd.read_csv(
            self.catalog_path,
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
        num_rows = dim_df.count()
        logger.info("✓ Songs catalog loaded (%d rows after dedup).", num_rows)
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

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port

    @classmethod
    def from_resource(cls, resource: RedisResource) -> "RedisProfileEnricher":
        return cls(host=resource.host, port=resource.port)

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
        resource: ClickHouseResource,
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
        # Cached client instance reused across all microbatches
        self._client: clickhouse_connect.driver.Client | None = None

    @property
    def client(self) -> clickhouse_connect.driver.Client:
        """Lazily initialize and reuse the ClickHouse client."""
        if self._client is None:
            self._client = self.resource.get_client()
        return self._client

    def write_batch(self, df: DataFrame, batch_id: int) -> None:
        """ForeachBatch handler called on every micro-batch trigger."""
        try:
            projected_df = project_playback_events_for_clickhouse(df)
            arrow_table = projected_df.toArrow()

            self.client.insert_arrow(self.table_name, arrow_table)
            logger.info(
                "✓ Batch %d: wrote %d enriched rows to ClickHouse table '%s'.",
                batch_id,
                arrow_table.num_rows,
                self.table_name,
            )
        except Exception as exc:
            logger.error(
                "✗ Batch %d: failed to write to ClickHouse table '%s': %s",
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
    """Streaming sink strategy for writing to Iceberg tables."""

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
