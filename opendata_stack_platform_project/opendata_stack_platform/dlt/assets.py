from collections.abc import Iterable
from dagster import AssetExecutionContext, AssetKey
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets, DagsterDltTranslator
import dlt
from opendata_stack_platform.dlt.sources.yellow_taxi_trip import yellow_taxi_trip
from opendata_stack_platform.partitions import monthly_partition


class YellowTaxiTripDagsterDltTranslator(DagsterDltTranslator):
    def get_asset_key(self, resource: DagsterDltResource) -> AssetKey:
        """Overrides asset key to be the dlt resource name."""
        return AssetKey("yellow_taxi_trip_bronz")

    def get_deps_asset_keys(self, resource: DagsterDltResource) -> Iterable[AssetKey]:
        yield AssetKey("taxi_trips_file")


@dlt_assets(
    dlt_source=yellow_taxi_trip(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="yellow_taxi_trip_bronz_pipeline",
        destination=dlt.destinations.duckdb("../data/nyc_database.duckdb"),
        dataset_name="yellow_taxi_trip_bronz",
        dev_mode=True,
        progress="log"
    ),
    name="dlt_pipeline",
    group_name="dlt_assets",
    dagster_dlt_translator=YellowTaxiTripDagsterDltTranslator(),
    partitions_def=monthly_partition

)
def dagster_yellow_taxi_trip_bronz_assets(
    context: AssetExecutionContext, dlt: DagsterDltResource
):
    partition_key = context.partition_key[:-3]
    yield from dlt.run(context=context, dlt_source=yellow_taxi_trip(partition_key=partition_key))

