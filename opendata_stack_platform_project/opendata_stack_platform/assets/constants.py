import os

BUCKET = "datalake"
S3_BUCKET_PREFIX = os.getenv("S3_BUCKET_PREFIX", "s3://datalake")


def get_path_for_env(path: str) -> str:
    """A utility method for Dagster University. Generates a path based on the environment.

    Args:
        path (str): The local path to the file.

    Returns:
        result_path (str): The path to the file, based on the environment.
    """
    return S3_BUCKET_PREFIX + path


TAXI_ZONES_FILE_PATH = get_path_for_env("/raw/taxi_zones.csv")


TAXI_TRIPS_RAW_KEY_TEMPLATE = (
    "raw/{dataset_type}/{dataset_type}_tripdata_{partition}.parquet"
)

START_DATE = "2024-01-01"
END_DATE = "2025-01-01"
