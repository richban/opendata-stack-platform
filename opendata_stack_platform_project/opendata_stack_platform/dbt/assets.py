import dagster as dg
from dagster_dbt import (
    DbtCliResource,
    dbt_assets,
)
from opendata_stack_platform.dbt.resources import opendata_stack_platform_dbt_project


@dbt_assets(
    manifest=opendata_stack_platform_dbt_project.manifest_path,
    backfill_policy=dg.BackfillPolicy.single_run(),
    project=opendata_stack_platform_dbt_project,
)
def dbt_non_partitioned_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from (
        dbt.cli(["build"], context=context)
        .stream()
        .fetch_row_counts()
        .fetch_column_metadata()
    )
