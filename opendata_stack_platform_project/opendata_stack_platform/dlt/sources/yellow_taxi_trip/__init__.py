from typing import Optional

from opendata_stack_platform.assets import constants
import dlt
from dlt.sources.filesystem import (
    filesystem,
    read_parquet,
)
from dlt.extract.source import DltSource

BUCKET_URL = "s3://datalake"
TAXI_TRIPS_TEMPLATE_FILE_PATH = "raw/yellow_taxi_trips/taxi_trips_{}.parquet"


@dlt.source(name="yellow_taxi_trip")
def yellow_taxi_trip(partition_key: Optional[str] = None) -> DltSource:
    """Source for yellow taxi trips data based on a specific file path format."""

    raw_files = filesystem(
        bucket_url=BUCKET_URL,
        file_glob=TAXI_TRIPS_TEMPLATE_FILE_PATH.format("*"),
        # files_per_page=1,
    )
    raw_files.apply_hints(write_disposition="append")
    raw_files.with_name("yellow_taxi_trip_bronz")

    if partition_key:
        raw_files.add_filter(lambda item: partition_key in item["file_name"])

    filesystem_pipe = raw_files | read_parquet()

    return filesystem_pipe


@dlt.source(name="taxi_trip_source")
def taxi_trip_source(
    dataset_type: str, partition_key: Optional[str] = None
) -> DltSource:
    """
    Source for taxi trips data (yellow, green, or FHV) based on a specific file path format.

    Args:
        dataset_type (str): The type of taxi dataset ("yellow", "green", "fhvhv").
        partition_key (Optional[str]): Optional partition key to filter files.
    """
    if dataset_type not in {"yellow", "green", "fhvhv"}:
        raise ValueError("dataset_type must be one of 'yellow', 'green', or 'fhvhv'.")

    # Construct file glob pattern for the dataset type
    file_glob = constants.TAXI_TRIPS_RAW_KEY_TEMPLATE.format(
        dataset_type=dataset_type, partition="*"
    )

    # Initialize the filesystem connector
    raw_files = filesystem(bucket_url=BUCKET_URL, file_glob=file_glob)
    raw_files.apply_hints(write_disposition="append")

    # Apply partition filter if provided
    if partition_key:
        raw_files.add_filter(lambda item: partition_key in item["file_name"])

    # Create a pipeline with filesystem and read_parquet
    filesystem_pipe = raw_files | read_parquet().with_name(
        f"{dataset_type}_taxi_trip_bronze"
    )

    return filesystem_pipe


if __name__ == "__main__":
    dlt_pipeline = dlt.pipeline(
        pipeline_name="green_taxi_trip_bronz_pipeline",
        destination=dlt.destinations.duckdb("../data/nyc_database.duckdb"),
        dataset_name="green_taxi_trip_bronz",
        dev_mode=True,
        progress="log",
    )
    # Run the pipeline, specifying a sample partition (e.g., "2023-01")
    load_info = dlt_pipeline.run(
        taxi_trip_source(dataset_type="green", partition_key="2024-01")
    )
    print(load_info)
