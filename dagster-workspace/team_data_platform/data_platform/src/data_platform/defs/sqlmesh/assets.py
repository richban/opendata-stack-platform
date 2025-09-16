import typing as t

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
)
from dagster_sqlmesh import SQLMeshResource, sqlmesh_assets

from data_platform.defs.sqlmesh.resources import sqlmesh_config
from data_platform.utils.environment_helpers import get_environment


@sqlmesh_assets(
    environment=get_environment(), config=sqlmesh_config, enabled_subsetting=True
)
def sqlmesh_project(
    context: AssetExecutionContext, sqlmesh: SQLMeshResource
) -> t.Iterator[MaterializeResult]:
    yield from sqlmesh.run(context)
