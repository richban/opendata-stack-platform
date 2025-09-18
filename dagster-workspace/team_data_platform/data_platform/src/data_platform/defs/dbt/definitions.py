from dagster import (
    Definitions,
    load_assets_from_modules,
)

from data_platform.defs.dbt import assets
from data_platform.defs.dbt.resources import dbt_resource

defs = Definitions(
    # assets=load_assets_from_modules([assets]),
    resources={
        "dbt": dbt_resource,
    },
)
