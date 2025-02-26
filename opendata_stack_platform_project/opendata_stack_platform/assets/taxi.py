from io import BytesIO

import polars as pl
import requests

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)
from dagster_aws.s3 import S3Resource

from opendata_stack_platform.assets import constants
from opendata_stack_platform.partitions import monthly_partition
from opendata_stack_platform.utils.download_and_upload_file import (
    download_and_upload_file,
)


@asset(group_name="raw_files")
def taxi_zones_file(s3: S3Resource) -> None:
    """
    The raw CSV file for the taxi zones dataset. Sourced from the NYC Open Data portal.
    """
    raw_taxi_zones = requests.get(
        "https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv"
    )

    s3_key = constants.TAXI_ZONES_FILE_PATH

    s3.get_client().put_object(
        Bucket=constants.BUCKET, Key=s3_key, Body=raw_taxi_zones.content
    )

    num_rows = len(pl.read_csv(BytesIO(raw_taxi_zones.content)))

    return MaterializeResult(metadata={"Number of records": MetadataValue.int(num_rows)})


@asset(partitions_def=monthly_partition, group_name="raw_files")
def yellow_taxi_trip_raw(context: AssetExecutionContext, s3: S3Resource) -> None:
    """
    The raw parquet files for the yellow taxi trips dataset. Sourced from the
        NYC Open Data portal.
    """
    download_and_upload_file(
        context,
        s3,
        dataset_type="yellow",
    )


@asset(partitions_def=monthly_partition, group_name="raw_files")
def green_taxi_trip_raw(context: AssetExecutionContext, s3: S3Resource) -> None:
    """
    The raw parquet files for the green taxi trips dataset. Sourced from the
        NYC Open Data portal.
    """
    download_and_upload_file(
        context,
        s3,
        dataset_type="green",
    )


@asset(partitions_def=monthly_partition, group_name="raw_files")
def fhvhv_trip_raw(context: AssetExecutionContext, s3: S3Resource) -> None:
    """
    The raw parquet files for the High Volume FHV trips dataset. Sourced from
        the NYC Open Data portal.
    """
    download_and_upload_file(
        context,
        s3,
        dataset_type="fhvhv",
    )
