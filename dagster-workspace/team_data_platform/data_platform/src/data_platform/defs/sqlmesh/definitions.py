
from dagster import (
    Definitions,
)

from data_platform.defs.sqlmesh import assets
from data_platform.defs.sqlmesh.resources import sqlmesh_resource

defs = Definitions(
    assets=[assets],
    resources={
        "sqlmesh": sqlmesh_resource,
    },
)
