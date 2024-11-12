from dagster import AssetSpec, AssetKey, EnvVar, asset, AssetExecutionContext, Failure
from botocore.exceptions import BotoCoreError, ClientError
from dagster_aws.s3 import S3Resource
import requests
import polars as pl
from opendata_stack_platform.assets import constants
from opendata_stack_platform.partitions import monthly_partition


# Define the source asset and point to the file in your data lake
source_portfolio_asset = AssetSpec(
    key=AssetKey(["portfolio_real_assets"]),
    metadata={
        "aws_account": EnvVar("AWS_ACCOUNT").get_value(),
        "s3_location": "s3://datalake/portfolio_real_assets.csv",
    },
    description=("Contains portfolio of physical assets."),
    group_name="external_assets",
    # partition_key = MultiPartitionKey({"client_id": "client12345", "year": "2024"})
).with_io_manager_key("polars_csv_io_manager")


@asset
def derived_asset_from_source(portfolio_real_assets: pl.LazyFrame) -> pl.DataFrame:
    """
    Derived asset that takes the source asset (loaded by the I/O manager) and processes it.
    """
    filtered_df = portfolio_real_assets.filter(pl.col("Value (USD)") > 100)
    return filtered_df.collect()


@asset(
    partitions_def=monthly_partition
)
def taxi_trips_file(context: AssetExecutionContext, s3: S3Resource) -> None:
    """
    The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """
    partition_date_str = context.partition_key
    partition_to_fetch = partition_date_str[:-3]

    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{partition_to_fetch}.parquet"
    s3_key = constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(partition_to_fetch)
    s3_bucket = constants.BUCKET

    # Logging the start of the download process
    context.log.info(f"Starting download for partition: {partition_to_fetch} from {url}")

    try:
        # Download the file
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for HTTP errors

        # Calculate file size in MiB
        file_size_mib = len(response.content) / (1024 * 1024)
        context.log.info(f"Downloaded data for {partition_to_fetch}, size: {file_size_mib:.2f} MiB")

        # Upload the file to S3
        s3.get_client().put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=response.content
        )

        context.log.info(f"Successfully uploaded {s3_key} to bucket {s3_bucket}")

    except requests.exceptions.RequestException as e:
        context.log.error(f"Failed to download file for partition {partition_to_fetch} from {url}: {e}")
        raise Failure(f"Download error for partition {partition_to_fetch}: {str(e)}") from e

    except (BotoCoreError, ClientError) as e:
        context.log.error(f"Failed to upload file to S3 for partition {partition_to_fetch}, key {s3_key}: {e}")
        raise Failure(f"S3 upload error for partition {partition_to_fetch}: {str(e)}") from e


@asset
def taxi_zones_file(s3: S3Resource) -> None:
    """
      The raw CSV file for the taxi zones dataset. Sourced from the NYC Open Data portal.
    """
    raw_taxi_zones = requests.get(
        "https://data.cityofnewyork.us/api/views/755u-8jsi/rows.csv?accessType=DOWNLOAD"
    )

    s3_key = constants.TAXI_ZONES_FILE_PATH

    s3.get_client().put_object(
        Bucket=constants.BUCKET,
        Key=s3_key,
        Body=raw_taxi_zones.content
    )
