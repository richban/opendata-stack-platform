import dagster as dg

from dagster import in_process_executor, load_assets_from_modules, mem_io_manager
from dagster_aws.s3 import S3Resource

from physical_risk_impact.defs import assets
from physical_risk_impact.defs.graphs import (
    dynamic_graph_calculation_climate_impact,
    dynamic_sensor_graph_calculation_climate_impact,
    graph_calculation_climate_impact,
)
from physical_risk_impact.defs.polars_csv_io_manager import PolarsCSVIOManager
from physical_risk_impact.defs.sensors import make_s3_sensor

# Resource config for interacting with MinIO or S3
storage_options = {
    "key": dg.EnvVar("AWS_ACCESS_KEY_ID").get_value(),
    "secret": dg.EnvVar("AWS_SECRET_ACCESS_KEY").get_value(),
    "client_kwargs": {
        "endpoint_url": dg.EnvVar("AWS_ENDPOINT_URL").get_value(),
        "region_name": dg.EnvVar("AWS_REGION").get_value(),
    },
}


polars_resource = PolarsCSVIOManager(
    base_dir="s3://datalake/", storage_options=storage_options
)

s3_resource = S3Resource(
    aws_access_key_id=dg.EnvVar("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=dg.EnvVar("AWS_SECRET_ACCESS_KEY"),
    endpoint_url=dg.EnvVar("AWS_ENDPOINT_URL"),
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

defs = dg.Definitions(
    assets=[*load_assets_from_modules([assets]), assets.source_portfolio_asset],
    sensors=[make_s3_sensor(dynamic_sensor_job)],
    resources={
        "polars_csv_io_manager": polars_resource,
        "mem_io_manager": mem_io_manager,
        "s3": s3_resource,
    },
    jobs=[
        graph_calculation_climate_impact_job,
        dynamic_graph_calculation_climate_impact_job,
        dynamic_sensor_job,
    ],
)
