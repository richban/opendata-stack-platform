from io import BytesIO

import polars as pl
import requests
from dagster import (
    AssetExecutionContext,
    EnvVar,
    MaterializeResult,
    MetadataValue,
    asset,
)
from dagster_aws.s3 import S3Resource
from dagster_duckdb import DuckDBResource
from dagster_snowflake import SnowflakeResource

from data_platform.defs.taxi import constants
from data_platform.defs.taxi.partitions import monthly_partition
from data_platform.utils.download_and_upload_file import (
    download_and_upload_file,
)
from data_platform.utils.environment_helpers import get_compute_kind, get_environment


@asset(group_name="raw_files", kinds={"csv"})
def taxi_zone_lookup_raw(
    context: AssetExecutionContext, s3: S3Resource
) -> MaterializeResult:
    """
    The raw CSV file for the taxi zones dataset. Sourced from the NYC Open Data portal.
    """
    raw_taxi_zones = requests.get(
        "https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv"
    )

    s3_key = constants.TAXI_ZONES_FILE_PATH

    context.log.info(f"Saving taxi zone lookup at: {constants.BUCKET}/{s3_key}")

    s3.get_client().put_object(
        Bucket=constants.BUCKET, Key=s3_key, Body=raw_taxi_zones.content
    )

    num_rows = len(pl.read_csv(BytesIO(raw_taxi_zones.content)))

    return MaterializeResult(
        metadata={"Number of records": MetadataValue.int(num_rows)}
    )


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
    group_name="ingested_taxi_trip_bronze",
    compute_kind=get_compute_kind(),
    key_prefix=["main"],
)
def taxi_zone_lookup(
    context: AssetExecutionContext,
    duckdb_resource: DuckDBResource,
    snowflake_resource: SnowflakeResource,
):
    """The raw taxi zones dataset, loaded into database (environment-aware)."""
    environment = get_environment()

    if environment == "prod":
        # Production: Use Snowflake
        context.log.info("Loading taxi zone lookup into Snowflake")

        db = EnvVar("SNOWFLAKE_DATABASE").get_value()

        query = f"""
            CREATE OR REPLACE TABLE {db}.MAIN.taxi_zone_lookup AS (
                WITH ranked_zones AS (
                    SELECT
                        $6::INT as zone_id,           -- LocationID is column 6
                        $5::VARCHAR as zone_name,     -- zone is column 5  
                        $7::VARCHAR as borough_name,  -- borough is column 7
                        TO_GEOMETRY($3) as geom_data, -- the_geom is column 3
                        ST_AREA(TO_GEOMETRY($3)) as area_size,
                        ST_PERIMETER(TO_GEOMETRY($3)) as perimeter_length,
                        ROW_NUMBER() OVER (PARTITION BY $1 ORDER BY $1) as row_num
                    FROM @RAW_STAGE/taxi_zones.csv
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
        """

        with snowflake_resource.get_connection() as conn:
            with conn.cursor() as cur:
                # Set schema context to PUBLIC (where RAW_STAGE exists)
                cur.execute(f"USE SCHEMA {db}.PUBLIC;")
                context.log.info("Using PUBLIC schema for stage access")

                # Create MAIN schema
                cur.execute("CREATE SCHEMA IF NOT EXISTS MAIN;")
                context.log.info("Schema MAIN created or already exists")

                # Create table in MAIN schema
                cur.execute(query)
                context.log.info("Taxi zone lookup successfully loaded into Snowflake")

    else:
        # Development: Use DuckDB with spatial functions
        context.log.info("Loading taxi zone lookup into DuckDB")

        query = f"""
            CREATE OR REPLACE TABLE taxi_zone_lookup AS (
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
        """

        with duckdb_resource.get_connection() as conn:
            conn.execute(query)
            context.log.info("Taxi zone lookup successfully loaded into DuckDB")
