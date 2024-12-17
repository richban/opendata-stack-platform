import dagster as dg
import json
from typing import Any, Mapping, Optional
from dagster_dbt import (
    DbtCliResource,
    dbt_assets,
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
)
from opendata_stack_platform.partitions import monthly_partition
from opendata_stack_platform.dbt.resources import opendata_stack_platform_dbt_project


class DbtConfig(dg.Config):
    full_refresh: bool = False


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        if dbt_resource_props["resource_type"] == "snapshot":
            return "snapshots"
        # Same logic that sets the custom schema in macros/get_custom_schema.sql
        asset_path = dbt_resource_props["fqn"][1:-1]
        if asset_path:
            return "_".join(asset_path)
        return "default"

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> dg.AssetKey:
        resource_database = dbt_resource_props["database"]
        resource_schema = dbt_resource_props["schema"]
        resource_name = dbt_resource_props["name"]
        resource_type = dbt_resource_props["resource_type"]

        # if metadata has been provided in the yaml use that, otherwise construct key
        if (
            resource_type == "source"
            and "meta" in dbt_resource_props
            and "dagster" in dbt_resource_props["meta"]
            and "asset_key" in dbt_resource_props["meta"]["dagster"]
        ):
            return dg.AssetKey(dbt_resource_props["meta"]["dagster"]["asset_key"])

        return dg.AssetKey([resource_database, resource_schema, resource_name])

    def get_metadata(self, dbt_resource_props: Mapping[str, Any]) -> Mapping[str, Any]:
        url_metadata = {}
        if dbt_resource_props["resource_type"] == "model":
            url_metadata = {
                "url": dg.MetadataValue.url(
                    "/".join(
                        [
                            dbt_resource_props["schema"].upper(),
                            "table",
                            dbt_resource_props["name"].upper(),
                        ]
                    )
                )
            }

        return {
            **super().get_metadata(dbt_resource_props),
            **url_metadata,
        }


@dbt_assets(
    manifest=opendata_stack_platform_dbt_project.manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator(
        settings=DagsterDbtTranslatorSettings(enable_code_references=True)
    ),
    partitions_def=monthly_partition,
    backfill_policy=dg.BackfillPolicy.single_run(),
    project=opendata_stack_platform_dbt_project,
)
def dbt_partitioned_models(
    context: dg.AssetExecutionContext, dbt: DbtCliResource, config: DbtConfig
):
    context.log.info(f"partition_key: {context.partition_key}")
    # Pass the partition date directly to dbt
    dbt_vars = {
        "date_partition": context.partition_key,
    }
    args = ["build", "--vars", json.dumps(dbt_vars)]

    if config.full_refresh:
        args = ["build", "--full-refresh"]

    yield from (
        dbt.cli(args, context=context)
        .stream()
        .fetch_row_counts()
        .fetch_column_metadata()
    )
