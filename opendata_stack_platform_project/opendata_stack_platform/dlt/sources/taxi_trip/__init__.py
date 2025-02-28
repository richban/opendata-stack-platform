import logging

from typing import Optional

import dlt

from dlt.extract.source import DltSource
from dlt.sources.filesystem import filesystem

from opendata_stack_platform.assets import constants
from opendata_stack_platform.dlt.sources.taxi_trip.utils import read_parquet_custom

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
            "tpep_pickup_datetime" if dataset_type == "yellow" else "lpep_pickup_datetime"
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
def taxi_trip_source(dataset_type: str, partition_key: Optional[str] = None) -> DltSource:
    """Source for taxi trips data (yellow, green, or FHV) based on file path.

    Args:
        dataset_type: Type of dataset ('yellow', 'green', or 'fhvhv')
        partition_key: Optional partition key for filtering data

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

    # Apply partition filter if provided
    if partition_key:
        # YYYY-MM-DD to YYYY-MM
        raw_files.add_filter(lambda item: partition_key[:-3] in item["file_name"])

    # Create source with transformations
    source = (
        raw_files
        | read_parquet_custom(
            partition_key=partition_key,
            key_columns=key_columns,
        )
    ).with_name(f"{dataset_type}_taxi_trip_bronze")

    # Apply write configuration hints
    source.apply_hints(
        write_disposition="merge", primary_key=natural_key, merge_key=natural_key
    )

    return source


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    dlt_pipeline = dlt.pipeline(
        pipeline_name="green_taxi_trip_bronze_pipeline",
        destination=dlt.destinations.duckdb("../data/nyc_database.duckdb"),
        dataset_name="green_taxi_trip_bronze",
        dev_mode=True,
        progress="log",
    )
    # Run the pipeline, specifying a sample partition (e.g., "2024-01-01")
    load_info = dlt_pipeline.run(
        taxi_trip_source(dataset_type="green", partition_key="2024-01-01")
    )
    logger.info("Pipeline load info: %s", load_info)
