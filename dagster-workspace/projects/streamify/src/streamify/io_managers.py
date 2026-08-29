import logging
from typing import Protocol

import clickhouse_connect
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from streamify.defs.resources import ClickHouseResource
from streamify.transformations import project_playback_events_for_clickhouse

logger = logging.getLogger(__name__)


class StreamingIOManager(Protocol):
    def write(self, df: DataFrame, topic: str) -> StreamingQuery: ...


class ClickHouseIOManager:
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
