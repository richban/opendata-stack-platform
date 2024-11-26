from typing import Iterator
from dlt.sources.filesystem import FileItemDict
from pyarrow import parquet as pq
import dlt
import pyarrow


@dlt.transformer()
def read_parquet_custom(
    items: Iterator[FileItemDict],
    batch_size: int = 64_000,
) -> Iterator[pyarrow.RecordBatch]:
    """
    Reads Parquet file content and enriches it with file metadata using PyArrow RecordBatch.

    Args:
        items (Iterator[FileItemDict]): Iterator over file items.
        batch_size (int, optional): Maximum number of rows to process per batch, defaults to 64K.

    Yields:
        pyarrow.RecordBatch: Enriched RecordBatch with metadata.
    """
    for file_obj in items:
        file_name = file_obj["file_name"]
        with file_obj.open() as f:
            parquet_file = pq.ParquetFile(f)

            # Iterate over RecordBatch objects
            for batch in parquet_file.iter_batches(batch_size=batch_size):
                # Create a new column for metadata
                new_column = pyarrow.array([file_name] * len(batch))

                # Create a new RecordBatch with the existing columns and the new column
                batch_with_metadata = pyarrow.RecordBatch.from_arrays(
                    batch.columns + [new_column],
                    schema=batch.schema.append(
                        pyarrow.field("file_name", new_column.type)
                    ),
                )

                # Yield the enriched RecordBatch
                yield batch_with_metadata
