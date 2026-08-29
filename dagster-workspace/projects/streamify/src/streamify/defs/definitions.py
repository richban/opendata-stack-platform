"""Dagster definitions for Streamify.

Architecture:
- Streaming assets: Use Dagster Pipes with spark-submit for long-running jobs
- Batch assets: Use Spark Connect for direct PySpark API access

All configuration is managed via ConfigurableResources loaded from environment variables.
"""

import dagster as dg

from streamify.defs import (
    bronze_assets,
    sensors,
)
from streamify.defs.resources import (
    create_s3_resource,
    get_streaming_config,
    spark_resource,
)

streaming_config = get_streaming_config()

defs = dg.Definitions(
    assets=dg.load_assets_from_modules([bronze_assets]),
    sensors=[sensors.bronze_restart_sensor],
    jobs=[],
    resources={
        "spark": spark_resource,
        "s3": create_s3_resource(streaming_config),
        "streaming_config": streaming_config,
    },
)
