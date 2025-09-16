from dagster import (
    Definitions,
    EnvVar,
    load_assets_from_modules,
)
from dagster_aws.s3 import S3Resource

from data_platform.defs.taxi import assets
from data_platform.defs.taxi.resources import duckdb_resource, snowflake_resource

s3_resource = S3Resource(
    aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
    endpoint_url=EnvVar("AWS_ENDPOINT_URL"),
)

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    resources={
        "s3": s3_resource,
        "duckdb_resource": duckdb_resource,
        "snowflake_resource": snowflake_resource
    },
)
