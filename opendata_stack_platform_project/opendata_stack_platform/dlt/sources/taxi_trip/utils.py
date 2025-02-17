from collections.abc import Iterator
from datetime import date, datetime, timezone
from typing import Optional

import dlt
import pyarrow as pa

from dlt.sources.filesystem import FileItemDict
from pyarrow import parquet as pq


def add_partition_column(batch: pa.RecordBatch, partition_key: str) -> pa.RecordBatch:
    """
    adds a new column with the partition key to the given recordbatch.

    args:
        batch (pa.RecordBatch): the original recordbatch.
        partition_key (str): the partition key to add as a new column.
    returns:
        pa.RecordBatch: the recordbatch with the new column added.
    """
    # Convert YYYY-MM-DD string to datetime.date first
    partition_date = (
        datetime.strptime(partition_key, "%Y-%m-%d").replace(tzinfo=timezone.utc).date()
    )
    # Convert to days since epoch (1970-01-01)
    epoch = date(1970, 1, 1)
    days_since_epoch = (partition_date - epoch).days

    # Create an array of the same value repeated for the length of the batch
    date_array = pa.array([days_since_epoch] * len(batch), type=pa.date32())
    new_batch = batch.append_column("partition_key", date_array)
    return new_batch


@dlt.transformer(standalone=True)
def read_parquet_custom(
    items: Iterator[FileItemDict],
    partition_key: Optional[str] = None,
    batch_size: int = 64_000,
) -> Iterator[pa.RecordBatch]:
    """
    Reads Parquet file content and enriches it with file metadata using
        PyArrow RecordBatch.

    Args:
        items (Iterator[FileItemDict]): Iterator over file items.
        batch_size (int, optional): Maximum number of rows to process per batch

    Yields:
        pyarrow.RecordBatch: Enriched RecordBatch with metadata.
    """
    for file_obj in items:
        with file_obj.open() as f:
            parquet_file = pq.ParquetFile(f)
            # Iterate over RecordBatch objects
            for batch in parquet_file.iter_batches(batch_size=batch_size):
                # Create a new RecordBatch with the existing columns and the new column
                batch_with_metadata = add_partition_column(batch, partition_key)
                # Yield the enriched RecordBatch
                yield batch_with_metadata
