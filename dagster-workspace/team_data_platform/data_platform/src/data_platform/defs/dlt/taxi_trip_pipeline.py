"""Local invocation of taxi_trip pipeline

USAGE:

    cd opendata_stack_platform_project/
    python -m opendata_stack_platform.dlt.taxi_trip_local

NOTE:
    .dlt/ has to be present in opendata_stack_platform_project/
"""

import os

import dlt

from data_platform.defs.dlt.sources.taxi_trip import taxi_trip_source
from data_platform.utils.paths import get_duckdb_path


def create_taxi_trip_pipeline(dataset_type: str):
    """Create a pipeline for taxi trip data.

    Args:
        dataset_type: Type of taxi data (yellow, green, fhvhv)
    """
    environment = os.getenv("ENVIRONMENT", "dev")

    if environment == "prod":
        # Production: Use Snowflake with local MinIO staging
        pipeline = dlt.pipeline(
            pipeline_name=f"{dataset_type}_taxi_trip_bronze_pipeline",
            destination="snowflake",
            # staging="filesystem",  # FIX: Needs to stagge files on S3; Connection Issues;
            dataset_name=f"BRONZE_{dataset_type.upper()}",
            progress="log",
        )
    else:
        # Development: Use DuckDB
        duckdb_path = str(get_duckdb_path())
        pipeline = dlt.pipeline(
            pipeline_name=f"{dataset_type}_taxi_trip_bronze_pipeline",
            destination=dlt.destinations.duckdb(
                duckdb_path,
                dataset_name=f"bronze_{dataset_type}",
            ),
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
