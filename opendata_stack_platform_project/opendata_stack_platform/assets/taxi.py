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
from dagster_duckdb import DuckDBResource

from opendata_stack_platform.assets import constants
from opendata_stack_platform.partitions import monthly_partition
from opendata_stack_platform.utils.download_and_upload_file import (
    download_and_upload_file,
)


@asset(group_name="raw_files", kinds={"csv"})
def taxi_zone_lookup_raw(s3: S3Resource) -> None:
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


@asset(partitions_def=monthly_partition, group_name="raw_files", kinds={"parquet"})
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


@asset(partitions_def=monthly_partition, group_name="raw_files", kinds={"parquet"})
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


@asset(partitions_def=monthly_partition, group_name="raw_files", kinds={"parquet"})
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


@asset(
    deps=["taxi_zone_lookup_raw"],
    group_name="ingested_taxi_trip_silver",
    compute_kind="DuckDB",
    key_prefix=["nyc_database", "silver"],
)
def taxi_zone_lookup(context: AssetExecutionContext, duckdb_resource: DuckDBResource):
    """The raw taxi zones dataset, loaded into a DuckDB database."""
    query = f"""
        -- Install and load the spatial extension
        INSTALL spatial;
        LOAD spatial;

        create or replace table taxi_zone_lookup as (
            WITH ranked_zones AS (
                SELECT
                    LocationID as zone_id,
                    zone AS zone_name,
                    borough AS borough_name,
                    st_geomfromtext(the_geom) as geom_data,
                    st_area(st_geomfromtext(the_geom)) as area_size,
                    st_perimeter(st_geomfromtext(the_geom)) as perimeter_length,
                    ROW_NUMBER() OVER (PARTITION BY LocationID ORDER BY LocationID) as row_num
                FROM '{constants.get_path_for_env(constants.TAXI_ZONES_FILE_PATH)}'
            )
            SELECT
                zone_id,
                zone_name,
                borough_name,
                geom_data,
                area_size,
                perimeter_length
            FROM ranked_zones
            WHERE row_num = 1
        );
    """  # noqa: E501

    with duckdb_resource.get_connection() as conn:
        conn.execute(query)
