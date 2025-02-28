from dagster import (
    Definitions,
    EnvVar,
    in_process_executor,
    load_assets_from_modules,
    load_assets_from_package_module,
    mem_io_manager,
)
from dagster_aws.s3 import S3Resource
from dagster_dlt import DagsterDltResource

from opendata_stack_platform import assets
from opendata_stack_platform.dbt import assets as dbt_assets
from opendata_stack_platform.dbt.resources import dbt_resource
from opendata_stack_platform.dlt import assets as dlt_assets
from opendata_stack_platform.graphs import (
    dynamic_graph_calculation_climate_impact,
    dynamic_sensor_graph_calculation_climate_impact,
    graph_calculation_climate_impact,
)
from opendata_stack_platform.resources import duckdb_resource
from opendata_stack_platform.resources.polars_csv_io_manager import PolarsCSVIOManager
from opendata_stack_platform.sensors import make_s3_sensor

dlt_assets = load_assets_from_modules([dlt_assets])
dbt_assets = load_assets_from_modules([dbt_assets])
core_assets = load_assets_from_package_module(assets)

# Resource config for interacting with MinIO or S3
storage_options = {
    "key": EnvVar("AWS_ACCESS_KEY_ID").get_value(),
    "secret": EnvVar("AWS_SECRET_ACCESS_KEY").get_value(),
    "client_kwargs": {
        "endpoint_url": EnvVar("AWS_ENDPOINT_URL").get_value(),
        "region_name": EnvVar("AWS_REGION").get_value(),
    },
}


all_assets = [
    *dlt_assets,
    *dbt_assets,
    *core_assets,
    assets.core.source_portfolio_asset,
]

polars_resource = PolarsCSVIOManager(
    base_dir="s3://datalake/", storage_options=storage_options
)

s3_resource = S3Resource(
    aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
    endpoint_url=EnvVar("AWS_ENDPOINT_URL"),
)

graph_calculation_climate_impact_job = graph_calculation_climate_impact.to_job(
    name="calculate_climate_impact_job",
    description="Job to calculate climate impact based on custom input type via config.",
    config={
        "inputs": {
            "input_asset": {
                "latitude": 40.7128,
                "longitude": -121.4876,
                "market_value": 1000000.0,
            }
        }
    },
    resource_defs={"mem_io_manager": mem_io_manager},
    executor_def=in_process_executor,
)

dynamic_graph_calculation_climate_impact_job = (
    dynamic_graph_calculation_climate_impact.to_job(
        name="dynamic_job",
        description="Job to calculate impact based on an external asset.",
        resource_defs={
            "mem_io_manager": mem_io_manager,
            "polars_csv_io_manager": polars_resource,
        },
        executor_def=in_process_executor,
    )
)

dynamic_sensor_job = dynamic_sensor_graph_calculation_climate_impact.to_job(
    name="dynamic_sensor_job",
    description="Job that's triggered via a sensor when new files appear in S3.",
    resource_defs={
        "mem_io_manager": mem_io_manager,
        "polars_csv_io_manager": polars_resource,
        "s3": s3_resource,
    },
    executor_def=in_process_executor,
    config={
        "ops": {
            "input_split_portfolio_to_rows": {
                "inputs": {"df": {"key": "raw/"}},
            },
        }
    },
)


core_defs = Definitions(
    assets=all_assets,
    sensors=[make_s3_sensor(dynamic_sensor_job)],
    resources={
        "polars_csv_io_manager": polars_resource,
        "mem_io_manager": mem_io_manager,
        "s3": s3_resource,
        "duckdb_resource": duckdb_resource,
        "dlt_resource": DagsterDltResource(),
        "dbt": dbt_resource,
    },
    jobs=[
        graph_calculation_climate_impact_job,
        dynamic_graph_calculation_climate_impact_job,
        dynamic_sensor_job,
    ],
)
