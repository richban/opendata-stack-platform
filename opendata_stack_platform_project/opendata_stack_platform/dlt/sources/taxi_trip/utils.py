import hashlib
import json

from collections.abc import Iterator
from datetime import date, datetime, timezone

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


def add_row_hash(
    batch: pa.RecordBatch, key_columns: list[str], hash_column_name: str = "row_hash"
) -> pa.RecordBatch:
    """
    Add a hash column to a PyArrow RecordBatch based on selected columns.

    Args:
        batch: PyArrow RecordBatch to process
        key_columns: List of column names to include in hash
        hash_column_name: Name for the new hash column

    Returns:
        PyArrow RecordBatch with added hash column
    """
    # Filter out columns that don't exist in the batch
    existing_columns = [col for col in key_columns if col in batch.schema.names]

    if not existing_columns:
        raise ValueError(
            f"None of the key columns {key_columns} exist in the batch schema"
        )

    # Initialize a list to store hash values
    hash_values = []

    # Process each row directly using PyArrow
    for i in range(batch.num_rows):
        # Create a dictionary for this row
        row_dict = {}

        # Extract values for each column in this row
        for col in existing_columns:
            # Get the column array
            col_array = batch.column(batch.schema.get_field_index(col))
            # Get the value at this row index
            value = col_array[i].as_py()

            # Only include non-None values
            if value is not None:
                row_dict[col] = value

        # Convert to sorted JSON string for consistent hashing
        json_str = json.dumps(row_dict, sort_keys=True, default=str)

        # Create hash
        hash_obj = hashlib.md5(json_str.encode())
        hash_value = hash_obj.hexdigest()
        hash_values.append(hash_value)

    # Create PyArrow array from hash values
    hash_array = pa.array(hash_values, type=pa.string())

    # Add hash column to batch
    new_batch = batch.append_column(hash_column_name, hash_array)
    return new_batch


@dlt.transformer(standalone=True)
def read_parquet_custom(
    items: Iterator[FileItemDict],
    partition_key: str,
    key_columns: list[str],
    batch_size: int = 64_000,
) -> Iterator[pa.RecordBatch]:
    """
    Reads Parquet file content and enriches it with file metadata using
        PyArrow RecordBatch.

    Args:
        items (Iterator[FileItemDict]): Iterator over file items.
        partition_key (Optional[str]): Partition key to add to the data.
        key_columns (Optional[List[str]]): Columns to use for row hash calculation.
        batch_size (int, optional): Maximum number of rows to process per batch

    Yields:
        pyarrow.RecordBatch: Enriched RecordBatch with metadata.
    """
    for file_obj in items:
        with file_obj.open() as f:
            parquet_file = pq.ParquetFile(f)
            # Iterate over RecordBatch objects
            for raw_batch in parquet_file.iter_batches(batch_size=batch_size):
                # Add partition column
                processed_batch = add_partition_column(raw_batch, partition_key)

                # Add row hash
                processed_batch = add_row_hash(processed_batch, key_columns)

                # Yield the enriched RecordBatch
                yield processed_batch
