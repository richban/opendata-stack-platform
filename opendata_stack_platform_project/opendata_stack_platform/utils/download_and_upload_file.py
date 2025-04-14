import requests
from botocore.exceptions import BotoCoreError, ClientError
from dagster import AssetExecutionContext, Failure
from dagster_aws.s3 import S3Resource

from opendata_stack_platform.assets import constants


def download_and_upload_file(
    context: AssetExecutionContext,
    s3: S3Resource,
    dataset_type: str,
) -> None:
    """
    Downloads the dataset from the NYC Open Data portal and uploads it to S3.
    Requires a partition_key in the context in YYYY-MM-DD format, which is used
    to determine which month's data to download (YYYY-MM).

    Args:
        context: Dagster execution context containing partition_key (YYYY-MM-DD format).
                The partition_key is required and used to determine which month's data
                to download by truncating the day component.
        s3: S3Resource to interact with S3.
        dataset_type: Type of dataset (yellow, green, hvfhv).

    Example:
        With context.partition_key = "2024-01-01":
        - Downloads data for January 2024 (2024-01)
        - Stores with full date in S3 path for consistent partitioning
    """
    partition_key = context.partition_key  # YYYY-MM-DD
    partition_to_fetch = partition_key[:-3]  # YYYY-MM

    # Generate the URL and S3 key
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_type}_tripdata_{partition_to_fetch}.parquet"
    s3_key = constants.TAXI_TRIPS_RAW_KEY_TEMPLATE.format(
        dataset_type=dataset_type, partition=partition_key
    )
    s3_bucket = constants.BUCKET

    # Logging the start of the download process
    context.log.info(
        f"Starting download for {dataset_type} trips, "
        f"partition: {partition_to_fetch} from {url}"
    )

    try:
        # Download the file
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for HTTP errors

        # Calculate file size in MiB
        file_size_mib = len(response.content) / (1024 * 1024)
        context.log.info(
            f"Downloaded {dataset_type} data for {partition_key}, " f"size: {file_size_mib:.2f} MiB"
        )

        # Upload the file to S3
        s3.get_client().put_object(Bucket=s3_bucket, Key=s3_key, Body=response.content)
        context.log.info(
            f"Successfully uploaded {s3_key} to bucket " f"{s3_bucket} for {dataset_type} trips"
        )

    except requests.exceptions.RequestException as e:
        context.log.error(
            f"Failed to download {dataset_type} file for partition"
            f"{partition_to_fetch} from {url}: {e}"
        )
        raise Failure(
            f"Download error for {dataset_type} partition {partition_to_fetch}: {e!s}"
        ) from e

    except (BotoCoreError, ClientError) as e:
        context.log.error(
            f"Failed to upload {dataset_type} file to S3 for partition"
            f"{partition_to_fetch}, key {s3_key}: {e}"
        )
        raise Failure(
            f"S3 upload error for {dataset_type} partition {partition_to_fetch}: {e!s}"
        ) from e
