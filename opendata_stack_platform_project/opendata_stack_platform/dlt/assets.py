from dagster import AssetExecutionContext
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets
import dlt
from opendata_stack_platform.dlt.sources.yellow_taxi_trip import yellow_taxi_trip

@dlt_assets(
    dlt_source=yellow_taxi_trip(partition="2024-01-01"),
    dlt_pipeline = dlt.pipeline(
        pipeline_name="yellow_taxi_pipeline",
        destination=dlt.destinations.duckdb("../data/nyc_database.duckdb"),
        dataset_name="yellow_taxi_trip_raw",
        dev_mode=True,
    ),
    name="dlt_pipeline",
    group_name="dlt_assets",
)
def dagster_planetview_database_assets(
    context: AssetExecutionContext, dlt: DagsterDltResource
):
    yield from dlt.run(context=context)
