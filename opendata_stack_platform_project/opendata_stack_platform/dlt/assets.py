from collections.abc import Iterable
from dagster import AssetExecutionContext, AssetKey
from dagster_embedded_elt.dlt import (
    DagsterDltResource,
    dlt_assets,
    DagsterDltTranslator,
)
import dlt
from opendata_stack_platform.dlt.sources.taxi_trip import taxi_trip_source
from opendata_stack_platform.dlt.taxi_trip_pipeline import create_taxi_trip_pipeline
from opendata_stack_platform.partitions import monthly_partition


class TaxiTripDagsterDltTranslator(DagsterDltTranslator):
    def __init__(self, dataset_type: str, deps_asset_key: str):
        self.dataset_type = dataset_type
        self.deps_asset_key = deps_asset_key

    def get_asset_key(self, resource: DagsterDltResource) -> AssetKey:
        """Overrides asset key to be the dlt resource name."""
        return AssetKey(f"{self.dataset_type}_taxi_trip_bronze")

    def get_deps_asset_keys(self, resource: DagsterDltResource) -> Iterable[AssetKey]:
        yield AssetKey(self.deps_asset_key)


def create_taxi_trip_bronze_asset(dataset_type: str, deps_asset_key: str):
    """
    Creates a DLT asset definition for the specified dataset type.

    Args:
        dataset_type (str): The type of taxi dataset ("yellow", "green", "fhvhv").
    """

    @dlt_assets(
        dlt_source=taxi_trip_source(dataset_type=dataset_type),
        dlt_pipeline=create_taxi_trip_pipeline(dataset_type),
        name=f"{dataset_type}_taxi_trip_bronze",
        group_name="dlt_assets",
        dagster_dlt_translator=TaxiTripDagsterDltTranslator(dataset_type, deps_asset_key),
        partitions_def=monthly_partition,
    )
    def dagster_taxi_trip_bronze_asset(
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
            )
        )

    return dagster_taxi_trip_bronze_asset


# Define assets for each dataset type
dagster_yellow_taxi_trip_bronze_assets = create_taxi_trip_bronze_asset(
    dataset_type="yellow", deps_asset_key="yellow_taxi_trip_raw"
)
dagster_green_taxi_trip_bronze_assets = create_taxi_trip_bronze_asset(
    dataset_type="green", deps_asset_key="green_taxi_trip_raw"
)
dagster_fhv_taxi_trip_bronze_assets = create_taxi_trip_bronze_asset(
    dataset_type="fhvhv", deps_asset_key="fhvhv_trip_raw"
)
