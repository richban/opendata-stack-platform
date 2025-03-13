import json

from collections.abc import Mapping
from typing import Any, Optional

import dagster as dg

from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    dbt_assets,
)

from opendata_stack_platform.dbt.resources import opendata_stack_platform_dbt_project
from opendata_stack_platform.partitions import monthly_partition


class DbtConfig(dg.Config):
    """Configuration class for DBT execution.

    Attributes:
        full_refresh (bool): Flag to perform a full refresh of DBT models.
    """

    full_refresh: bool = False


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    """Custom translator for DBT resources to Dagster assets.

    This class extends DagsterDbtTranslator to provide custom logic for:
    - Grouping DBT resources
    - Generating asset keys
    - Adding metadata to assets

    The translator helps organize DBT resources (models, sources, snapshots) in
    Dagster's asset-based data orchestration system with custom grouping,
    flexible asset key generation, and enhanced metadata for better observability.
    """

    def get_group_name(
        self,
        dbt_resource_props: Mapping[str, Any],
    ) -> Optional[str]:
        """Determine the group name for a DBT resource.

        Args:
            dbt_resource_props: A mapping containing DBT resource properties.

        Returns:
            str | None: The group name for the resource. Returns 'snapshots' for
                snapshot resources, concatenated asset path for other resources,
                or 'default' if no path exists.

        Examples:
            >>> # For a snapshot resource
            >>> get_group_name({"resource_type": "snapshot"})
            'snapshots'

            >>> # For a model with path
            >>> get_group_name({
            ...     "resource_type": "model",
            ...     "fqn": ["my_project", "marketing", "users", "final_table"]
            ... })
            'marketing_users'

            >>> # For a model with no path
            >>> get_group_name({"resource_type": "model", "fqn": ["my_project"]})
            'default'
        """
        if dbt_resource_props["resource_type"] == "snapshot":
            return "snapshots"
        # Same logic that sets the custom schema in macros/get_custom_schema.sql
        asset_path = dbt_resource_props["fqn"][1:-1]
        if asset_path:
            return "_".join(asset_path)
        return "default"

    def get_asset_key(
        self,
        dbt_resource_props: Mapping[str, Any],
    ) -> dg.AssetKey:
        """Generate an asset key for a DBT resource.

        Args:
            dbt_resource_props: A mapping containing DBT resource properties.

        Returns:
            AssetKey: A Dagster asset key. For source resources with
                meta.dagster.asset_key defined, uses that value. Otherwise,
                constructs a key from database, schema, and name.

        Examples:
            >>> # For a regular model
            >>> get_asset_key({
            ...     "resource_type": "model",
            ...     "database": "prod",
            ...     "schema": "marketing",
            ...     "name": "users"
            ... })
            AssetKey(["prod", "marketing", "users"])

            >>> # For a source with custom asset key in meta
            >>> get_asset_key({
            ...     "resource_type": "source",
            ...     "database": "prod",
            ...     "schema": "raw",
            ...     "name": "users",
            ...     "meta": {
            ...         "dagster": {
            ...             "asset_key": ["external", "users_source"]
            ...         }
            ...     }
            ... })
            AssetKey(["external", "users_source"])
        """
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

        # For sources in the silver layer (taxi trip data)
        if resource_type == "source" and resource_schema.startswith("silver_"):
            # Extract taxi type (yellow, green, fhvhv) from schema name
            taxi_type = resource_schema.replace("silver_", "")
            return dg.AssetKey(["nyc_database", "silver", f"{taxi_type}_taxi_trip"])

        # For models in the gold layer
        if resource_type == "model" and "fqn" in dbt_resource_props:
            model_path = dbt_resource_props["fqn"]
            # Check if this is a gold model
            if len(model_path) > 1 and model_path[1] == "gold":
                return dg.AssetKey(["nyc_database", "gold", resource_name])

        # Default case - use the original structure
        return dg.AssetKey([resource_database, resource_schema, resource_name])

    def get_metadata(
        self,
        dbt_resource_props: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """Generate metadata for a DBT resource.

        Args:
            dbt_resource_props: A mapping containing DBT resource properties.

        Returns:
            dict: A mapping containing metadata. For model resources, includes a URL
                constructed from the schema and name. Combines this with parent class
                metadata.

        Examples:
            >>> # For a DBT model
            >>> get_metadata({
            ...     "resource_type": "model",
            ...     "schema": "marketing",
            ...     "name": "users"
            ... })
            {
                # Parent class metadata
                ...,
                # Custom URL metadata
                "url": MetadataValue.url("MARKETING/table/USERS")
            }

            >>> # For a non-model resource (e.g., source)
            >>> get_metadata({
            ...     "resource_type": "source",
            ...     "schema": "raw",
            ...     "name": "users"
            ... })
            {
                # Only parent class metadata
                ...
            }
        """
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
    context: dg.AssetExecutionContext,
    dbt: DbtCliResource,
    config: DbtConfig,
) -> None:
    """Execute DBT models with monthly partitioning in Dagster.

    Execute DBT models with monthly partitioning in Dagster, providing:
    - Monthly partitioning of data processing
    - Code reference tracking for better observability
    - Single-run backfill policy for efficient historical processing
    - Partition-aware variable passing to DBT

    The function executes 'dbt build' with partition-specific variables,
    allowing DBT models to process data for specific time periods.

    Args:
        context: Dagster execution context containing partition information
            and logging capabilities
        dbt: DBT CLI resource for executing DBT commands
        config: Configuration object with full_refresh option

    Yields:
        Generator yielding DBT CLI execution results with:
        - Row count information
        - Column metadata
        - Streaming output for real-time logging

    Examples:
        Regular incremental build for a partition:
            >>> dbt_partitioned_models(context, dbt, DbtConfig(full_refresh=False))
            # Executes: dbt build --vars '{"partition_key": "2024-01"}'

        Full refresh build:
            >>> dbt_partitioned_models(context, dbt, DbtConfig(full_refresh=True))
            # Executes: dbt build --full-refresh

    Notes:
        - Uses CustomDagsterDbtTranslator for asset organization
        - Enables code references for better debugging and lineage tracking
        - Implements single-run backfill for efficient historical processing
        - Partition key is passed to DBT as a variable for time-based filtering
    """
    context.log.info(f"partition_key: {context.partition_key}")
    # Pass the partition date directly to dbt
    dbt_vars = {
        "partition_key": context.partition_key,
    }
    args = ["build", "--vars", json.dumps(dbt_vars)]

    if config.full_refresh:
        args = ["build", "--full-refresh"]

    yield from (
        dbt.cli(args, context=context).stream().fetch_row_counts().fetch_column_metadata()
    )
