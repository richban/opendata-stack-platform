"""Local invocation of yellow_taxi_trip pipeline

USAGE:

    cd opendata_stack_platform_project/
    python -m opendata_stack_platform.dlt.yellow_taxi_trip_local

NOTE:
    .dlt/ has to be present in opendata_stack_platform_project/
"""

import dlt
from opendata_stack_platform.dlt.sources.yellow_taxi_trip import yellow_taxi_trip


if __name__ == "__main__":
    dlt_pipeline = dlt.pipeline(
        pipeline_name="yellow_taxi_trip_pipeline",
        destination=dlt.destinations.duckdb("../data/nyc_database.duckdb"),
        dataset_name="yellow_taxi_trip_raw",
        dev_mode=True,
    )
    # Run the pipeline, specifying a sample partition (e.g., "2023-01")
    load_info = dlt_pipeline.run(yellow_taxi_trip(partition="2024-01-01"))
    print(load_info)
