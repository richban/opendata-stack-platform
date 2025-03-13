from collections.abc import Iterable

from dagster import AssetExecutionContext, AssetKey
from dagster_dlt import (
    DagsterDltResource,
    DagsterDltTranslator,
    dlt_assets,
)

from opendata_stack_platform.dlt.sources.taxi_trip import taxi_trip_source
from opendata_stack_platform.dlt.taxi_trip_pipeline import create_taxi_trip_pipeline
from opendata_stack_platform.partitions import monthly_partition


class TaxiTripDagsterDltTranslator(DagsterDltTranslator):
    def __init__(self, dataset_type: str, deps_asset_key: str):
        self.dataset_type = dataset_type
        self.deps_asset_key = deps_asset_key

    def get_asset_key(self, resource: DagsterDltResource) -> AssetKey:
        """Overrides asset key to be the dlt resource name."""
        return AssetKey(["nyc_database", "silver", self.dataset_type, "taxi_trip"])

    def get_deps_asset_keys(self, resource: DagsterDltResource) -> Iterable[AssetKey]:
        """Get the dependent asset keys for this DLT pipeline.

        This method yields the asset keys that this pipeline depends on. In this case,
        it yields the key specified in deps_asset_key, which represents the raw data
        source that needs to be processed.

        Args:
            resource: The DagsterDltResource instance.

        Returns:
            Iterable[AssetKey]: An iterable containing the dependent asset keys.
        """
        yield AssetKey(self.deps_asset_key)


def create_taxi_trip_silver_asset(dataset_type: str, deps_asset_key: str):
    """
    Creates a DLT asset definition for the specified dataset type.

    Args:
        dataset_type (str): The type of taxi dataset ("yellow", "green", "fhvhv").
    """

    @dlt_assets(
        dlt_source=taxi_trip_source(dataset_type=dataset_type),
        dlt_pipeline=create_taxi_trip_pipeline(dataset_type),
        name=f"{dataset_type}_taxi_trip_silver",
        group_name="ingested_taxi_trip_silver",
        dagster_dlt_translator=TaxiTripDagsterDltTranslator(dataset_type, deps_asset_key),
        partitions_def=monthly_partition,
    )
    def dagster_taxi_trip_silver_asset(
        context: AssetExecutionContext, dlt_resource: DagsterDltResource
    ):
        context.log.info(
            f"dataset_type: {dataset_type} partition_key: {context.partition_key}"
        )

        # Run pipeline with merge config from source
        yield from dlt_resource.run(
            context=context,
            dlt_source=taxi_trip_source(
                dataset_type=dataset_type, partition_key=context.partition_key
            ),
        )

    return dagster_taxi_trip_silver_asset


# Define assets for each dataset type
dagster_yellow_taxi_trip_silver_assets = create_taxi_trip_silver_asset(
    dataset_type="yellow", deps_asset_key="yellow_taxi_trip_raw"
)
dagster_green_taxi_trip_silver_assets = create_taxi_trip_silver_asset(
    dataset_type="green", deps_asset_key="green_taxi_trip_raw"
)
dagster_fhv_taxi_trip_silver_assets = create_taxi_trip_silver_asset(
    dataset_type="fhvhv", deps_asset_key="fhvhv_trip_raw"
)
