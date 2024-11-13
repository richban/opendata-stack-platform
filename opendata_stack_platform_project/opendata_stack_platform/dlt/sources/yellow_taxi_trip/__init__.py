from typing import Optional

import dlt
from dlt.sources.filesystem import (
    filesystem,
    read_parquet,
)
from dlt.extract.source import DltSource

BUCKET_URL = "s3://datalake"
TAXI_TRIPS_TEMPLATE_FILE_PATH = "raw/yellow_taxi_trips/taxi_trips_{}.parquet"


@dlt.source(name="yellow_taxi_trip")
def yellow_taxi_trip(partition: Optional[str] = None) -> DltSource:
    """Source for yellow taxi trips data based on a specific file path format."""

    raw_files = filesystem(
        bucket_url=BUCKET_URL, file_glob=TAXI_TRIPS_TEMPLATE_FILE_PATH.format("*")
    )
    raw_files.apply_hints(write_disposition="merge", merge_key="date")
    raw_files.with_name("yellow_taxi_trip_bronz")

    if partition:
        raw_files.add_filter(lambda item: partition in item["file_name"])

    filesystem_pipe = raw_files | read_parquet()

    return filesystem_pipe
