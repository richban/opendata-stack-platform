from dagster import (
    Definitions,
    load_assets_from_modules,
)

from data_platform.defs.taxi import assets
from data_platform.defs.taxi.resources import (
    duckdb_resource,
    s3_resource,
    snowflake_resource,
)

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    resources={
        "s3": s3_resource,
        "duckdb_resource": duckdb_resource,
        "snowflake_resource": snowflake_resource,
    },
)
