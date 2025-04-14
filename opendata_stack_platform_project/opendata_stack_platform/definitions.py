from dagster import (
    Definitions,
    EnvVar,
    load_assets_from_modules,
    load_assets_from_package_module,
)
from dagster_aws.s3 import S3Resource
from dagster_dlt import DagsterDltResource

from opendata_stack_platform import assets
from opendata_stack_platform.dbt import assets as dbt_assets
from opendata_stack_platform.dbt.resources import dbt_resource
from opendata_stack_platform.dlt import assets as dlt_assets
from opendata_stack_platform.resources import duckdb_resource

dlt_assets = load_assets_from_modules([dlt_assets])
dbt_assets = load_assets_from_modules([dbt_assets])
core_assets = load_assets_from_package_module(assets)

# Resource config for interacting with MinIO or S3
storage_options = {
    "key": EnvVar("AWS_ACCESS_KEY_ID").get_value(),
    "secret": EnvVar("AWS_SECRET_ACCESS_KEY").get_value(),
    "client_kwargs": {
        "endpoint_url": EnvVar("AWS_ENDPOINT_URL").get_value(),
        "region_name": EnvVar("AWS_REGION").get_value(),
    },
}


all_assets = [
    *dlt_assets,
    *dbt_assets,
    *core_assets,
]


s3_resource = S3Resource(
    aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
    endpoint_url=EnvVar("AWS_ENDPOINT_URL"),
)


defs = Definitions(
    assets=all_assets,
    resources={
        "s3": s3_resource,
        "duckdb_resource": duckdb_resource,
        "dlt_resource": DagsterDltResource(),
        "dbt": dbt_resource,
    },
)
