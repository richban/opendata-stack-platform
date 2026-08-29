import logging
from typing import Protocol

import clickhouse_connect
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from streamify.defs.resources import RedisResource
from streamify.schemas import ENRICHED_USER_PROFILE_SCHEMA
from streamify.defs.resources import ClickHouseResource
from streamify.transformations import (
    project_playback_events_for_clickhouse,
    read_kafka_stream,
    write_iceberg_stream,
)
from collections.abc import Iterable
from typing import Protocol
import pyarrow as pa
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from streamify.defs.resources import RedisResource
from streamify.schemas import ENRICHED_USER_PROFILE_SCHEMA
from streamify.transformations import enrich_profiles_partition


logger = logging.getLogger(__name__)


class StreamingSource(Protocol):
    def read(self, spark: SparkSession) -> DataFrame: ...


class StreamingSink(Protocol):
    def write(self, df: DataFrame, topic: str) -> StreamingQuery: ...


class StreamingIOManager(StreamingSource, StreamingSink, Protocol): ...


class StreamTransformer(Protocol):
    def transform(self, df: DataFrame) -> DataFrame: ...


class KafkaSource:
    def __init__(self, bootstrap_servers: str, topic: str, max_offsets: int):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.max_offsets = max_offsets

    def read(self, spark: SparkSession) -> DataFrame:
        return read_kafka_stream(
            spark, self.bootstrap_servers, self.topic, self.max_offsets
        )


class ClickHouseSink:
    def __init__(
        self,
        resource: ClickHouseResource,
        table_name: str,
        checkpoint_path: str,
        trigger_interval: str = "10 seconds",
    ) -> None:
        self.resource = resource
        self.table_name = table_name
        self.checkpoint_path = checkpoint_path
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
            # transform batch
            projected_df = project_playback_events_for_clickhouse(df)
            arrow_table = projected_df.toArrow()

            # re-use cached client to insert
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

    def write(self, df: DataFrame, topic: str) -> StreamingQuery:
        """Start the Structured Streaming query."""
        chkpt = f"{self.checkpoint_path}/{topic}_clickhouse"
        logger.info(
            "Declaring ClickHouse sink → table=%s, checkpoint=%s (trigger=%s)...",
            self.table_name,
            chkpt,
            self.trigger_interval,
        )
        return (
            df.writeStream.trigger(processingTime=self.trigger_interval)
            .option("checkpointLocation", chkpt)
            .queryName(f"clickhouse_{topic}")
            .foreachBatch(self.write_batch)
            .start()
        )


class IcebergSink:
    def __init__(
        self,
        chkpt: str,
        query_name: str,
        table_name: str,
        trigger_interval: str = "30 seconds",
    ):
        self.chkpt = chkpt
        self.query_name = query_name
        self.table_name = table_name
        self.trigger_interval = trigger_interval

    def write(self, df: DataFrame) -> StreamingQuery:
        return write_iceberg_stream(
            df, self.chkpt, self.query_name, self.table_name, self.trigger_interval
        )


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
