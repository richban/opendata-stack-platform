import logging

from typing import Optional

import dlt

from dlt.extract.source import DltSource
from dlt.sources.filesystem import filesystem

from data_platform.defs.dlt.sources.taxi_trip.utils import read_parquet_custom
from data_platform.defs.taxi import constants

BUCKET_URL = "s3://datalake"


def get_key_columns_for_dataset(dataset_type: str) -> list[str]:
    """
    Get the list of columns to use for row hash calculation based on dataset type.

    Args:
        dataset_type (str): Type of dataset ('yellow', 'green', or 'fhvhv')

    Returns:
        List[str]: List of column names to use for row hash
    """
    if dataset_type not in ["yellow", "green", "fhvhv"]:
        raise ValueError("dataset_type must be one of 'yellow', 'green', or 'fhvhv'.")

    if dataset_type in ["yellow", "green"]:
        pickup_datetime = (
            "tpep_pickup_datetime"
            if dataset_type == "yellow"
            else "lpep_pickup_datetime"
        )
        return [
            pickup_datetime,
            "pu_location_id",
            "do_location_id",
            "partition_key",
        ]
    else:  # fhvhv
        return [
            "pickup_datetime",
            "pu_location_id",
            "do_location_id",
            "partition_key",
        ]


@dlt.source(name="taxi_trip_source")
def taxi_trip_source(
    dataset_type: str,
    partition_key: Optional[str] = None,
    partition_range: Optional[tuple[str, str]] = None,
) -> DltSource:
    """Source for taxi trips data (yellow, green, or FHV) based on file path.

    Args:
        dataset_type: Type of dataset ('yellow', 'green', or 'fhvhv')
        partition_key: Optional partition key for filtering data (single partition)
        partition_range: Optional tuple of (start, end) partition keys for backfills

    Returns:
        DltSource: A data source for the specified taxi trip type
    """
    if dataset_type not in ["yellow", "green", "fhvhv"]:
        raise ValueError("dataset_type must be one of 'yellow', 'green', or 'fhvhv'.")

    # Get key columns for row hash from utility function
    key_columns = get_key_columns_for_dataset(dataset_type)

    # Natural key is always the row hash
    natural_key = ["row_hash"]

    # Construct file glob pattern for the dataset type
    file_glob = constants.TAXI_TRIPS_RAW_KEY_TEMPLATE.format(
        dataset_type=dataset_type, partition="*"
    )

    # Initialize the filesystem connector
    raw_files = filesystem(bucket_url=BUCKET_URL, file_glob=file_glob)

    # Apply partition filter
    if partition_range:
        # For backfills with a date range
        start_date, end_date = partition_range

        # Simple one-line filter that checks if the file's date is in range
        raw_files.add_filter(
            lambda item: start_date
            <= item["file_name"].split("_")[-1].split(".")[0]
            <= end_date
        )
    elif partition_key:
        # Single partition case
        # YYYY-MM-DD to YYYY-MM
        partition_ym = partition_key[:-3]
        raw_files.add_filter(lambda item: partition_ym in item["file_name"])

    # Create source with transformations
    source = (
        raw_files
        | read_parquet_custom(
            key_columns=key_columns,
        )
    ).with_name(f"{dataset_type}_taxi_trip")

    # Apply write configuration hints
    source.apply_hints(
        write_disposition="merge", primary_key=natural_key, merge_key=natural_key
    )

    return source


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    dlt_pipeline = dlt.pipeline(
        pipeline_name="unified_taxi_trip_bronze_pipeline",
        destination=dlt.destinations.duckdb("../data/nyc_database.duckdb"),
        dataset_name="bronze/green/taxi_trip",
        dev_mode=True,
        progress="log",
    )
    # Run the pipeline, specifying a sample partition (e.g., "2024-01-01")
    load_info = dlt_pipeline.run(
        taxi_trip_source(dataset_type="green", partition_key="2024-01-01")
    )
    logger.info("Pipeline load info: %s", load_info)
