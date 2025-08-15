
from dagster import (
    Definitions,
)

from data_platform.defs.sqlmesh.assets import sqlmesh_project
from data_platform.defs.sqlmesh.resources import sqlmesh_resource

defs = Definitions(
    assets=[sqlmesh_project],
    resources={
        "sqlmesh": sqlmesh_resource,
    },
)
