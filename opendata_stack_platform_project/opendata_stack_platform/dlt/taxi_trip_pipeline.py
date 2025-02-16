"""Local invocation of taxi_trip pipeline

USAGE:

    cd opendata_stack_platform_project/
    python -m opendata_stack_platform.dlt.taxi_trip_local

NOTE:
    .dlt/ has to be present in opendata_stack_platform_project/
"""

import dlt
from opendata_stack_platform.dlt.sources.taxi_trip import taxi_trip_source


def create_taxi_trip_pipeline(dataset_type: str):
    """Create a pipeline for taxi trip data.
    
    Args:
        dataset_type: Type of taxi data (yellow, green, fhvhv)
    """
    # Define natural key based on dataset type
    if dataset_type in ["yellow", "green"]:
        natural_key = (
            "VendorID",
            "tpep_pickup_datetime" if dataset_type == "yellow" else "lpep_pickup_datetime",
            "PULocationID",
            "DOLocationID",
            "partition_key"
        )
    else:  # fhvhv
        natural_key = (
            "hvfhs_license_num",
            "pickup_datetime",
            "PULocationID",
            "DOLocationID",
            "partition_key"
        )

    pipeline = dlt.pipeline(
        pipeline_name=f"{dataset_type}_taxi_trip_bronze_pipeline",
        destination=dlt.destinations.duckdb(
            "../data/nyc_database.duckdb",
            table_name=f"{dataset_type}_taxi_trip_bronze",
            primary_key=natural_key,  # For deduplication in staging
            merge_key=natural_key,  # Use same key for delete-insert matching
            write_disposition="merge",  # Use merge with delete-insert strategy
        ),
        dataset_name=f"{dataset_type}_taxi_trip_bronze",
        progress="log",
    )

    return pipeline


if __name__ == "__main__":
    # Example of running the pipeline directly (for testing)
    pipeline = create_taxi_trip_pipeline("green")
    load_info = pipeline.run(
        taxi_trip_source(dataset_type="green", partition_key="2024-02")
    )
    print(load_info)
