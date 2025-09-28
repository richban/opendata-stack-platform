import typing as t

from dagster import (
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
)
from dagster_sqlmesh import SQLMeshDagsterTranslator, SQLMeshResource, sqlmesh_assets
from sqlglot import exp
from sqlmesh.core.context import Context

from data_platform.defs.sqlmesh.resources import sqlmesh_config
from data_platform.utils.environment_helpers import get_environment


class CustomSQLMeshTranslator(SQLMeshDagsterTranslator):
    """Custom translator to align SQLMesh asset keys with DLT asset keys."""

    def get_asset_key(self, context: Context, fqn: str) -> AssetKey:
        """Override asset key generation to match DLT asset structure."""
        table = exp.to_table(fqn)
        # Skip the catalog/gateway (e.g., duckdb) and use only db.name (schema.table)
        return AssetKey([table.db, table.name])

    def get_group_name(self, context, model):
        """Group SQLMesh assets together."""
        return "sqlmesh_transformations"


@sqlmesh_assets(
    environment=get_environment(),
    config=sqlmesh_config,
    enabled_subsetting=True,
)
def sqlmesh_project(
    context: AssetExecutionContext, sqlmesh: SQLMeshResource
) -> t.Iterator[MaterializeResult]:
    yield from sqlmesh.run(context)
