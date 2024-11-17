"""Local invocation of taxi_trip pipeline

USAGE:

    cd opendata_stack_platform_project/
    python -m opendata_stack_platform.dlt.taxi_trip_local

NOTE:
    .dlt/ has to be present in opendata_stack_platform_project/
"""

import dlt
from opendata_stack_platform.dlt.sources.taxi_trip import taxi_trip_source


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
        taxi_trip_source(dataset_type="green", partition_key="2024-02")
    )
    print(load_info)
