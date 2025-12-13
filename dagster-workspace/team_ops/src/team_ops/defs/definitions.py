"""Dagster definitions for Streamify.

Architecture:
- Streaming assets: Use Dagster Pipes with spark-submit for long-running jobs
- Batch assets: Use Spark Connect for direct PySpark API access

All configuration is managed via ConfigurableResources loaded from environment variables.
"""

import dagster as dg

from team_ops.defs import assets
from team_ops.defs.resources import (
    create_s3_resource,
    create_spark_resource,
    create_streaming_config,
)

defs = dg.Definitions(
    assets=dg.load_assets_from_modules([assets]),
    resources={
        "spark": create_spark_resource(),
        "s3": create_s3_resource(),
        "streaming_config": create_streaming_config(),
    },
)
