from io import BytesIO

import polars as pl
import requests

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    EnvVar,
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

# Define the source asset and point to the file in your data lake
source_portfolio_asset = AssetSpec(
    key=AssetKey(["portfolio_real_assets"]),
    metadata={
        "aws_account": EnvVar("AWS_ACCOUNT").get_value(),
        "s3_location": "s3://datalake/portfolio_real_assets.csv",
    },
    description=("Contains portfolio of physical assets."),
    group_name="external_assets",
).with_io_manager_key("polars_csv_io_manager")


# Define constants
MIN_VALUE_USD = 100


@asset
def derived_asset_from_source(portfolio_real_assets: pl.LazyFrame) -> pl.DataFrame:
    """Process the source asset by filtering based on minimum value threshold.

    Takes the portfolio real assets DataFrame and filters out entries below the minimum
    value threshold defined by MIN_VALUE_USD constant.

    Args:
        portfolio_real_assets: A LazyFrame containing portfolio asset data with a
            'Value (USD)' column.

    Returns:
        pl.DataFrame: A filtered DataFrame containing only assets above the minimum
            value threshold.
    """
    filtered_df = portfolio_real_assets.filter(pl.col("Value (USD)") > MIN_VALUE_USD)
    return filtered_df.collect()


@asset
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


@asset(partitions_def=monthly_partition)
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


@asset(partitions_def=monthly_partition)
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


@asset(partitions_def=monthly_partition)
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
