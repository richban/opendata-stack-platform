from dagster import (
    Definitions,
    load_assets_from_modules,
)
from dagster_dlt import DagsterDltResource

from data_platform.defs.dlt import assets
from data_platform.defs.dlt.resources import duckdb_resource

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    resources={
        "duckdb_resource": duckdb_resource,
        "dlt_resource": DagsterDltResource(),
    },
)
