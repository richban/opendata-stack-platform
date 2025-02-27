"""Local invocation of taxi_trip pipeline

USAGE:

    cd opendata_stack_platform_project/
    python -m opendata_stack_platform.dlt.taxi_trip_local

NOTE:
    .dlt/ has to be present in opendata_stack_platform_project/
"""

import dlt

from opendata_stack_platform.dlt.sources.taxi_trip import taxi_trip_source
from opendata_stack_platform.utils.paths import get_duckdb_path


def create_taxi_trip_pipeline(dataset_type: str):
    """Create a pipeline for taxi trip data.

    Args:
        dataset_type: Type of taxi data (yellow, green, fhvhv)
    """
    pipeline = dlt.pipeline(
        pipeline_name=f"{dataset_type}_taxi_trip_bronze_pipeline",
        destination=dlt.destinations.duckdb(
            str(get_duckdb_path()),
            table_name=f"{dataset_type}_taxi_trip_bronze",
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
    print(load_info)  # noqa: T201
