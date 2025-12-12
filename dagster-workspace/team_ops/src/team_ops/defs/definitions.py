"""Dagster definitions for Streamify.

Architecture:
- Streaming assets: Use Dagster Pipes with spark-submit for long-running jobs
- Batch assets: Use Spark Connect for direct PySpark API access

All configuration is managed via ConfigurableResources loaded from environment variables.
"""

import dagster as dg

from team_ops.defs import assets, streaming_assets
from team_ops.defs.resources import (
    create_s3_resource,
    create_spark_resource,
    create_streaming_config,
)

defs = dg.Definitions(
    assets=[
        streaming_assets.bronze_streaming_job,
        assets.silver_listen_events,
    ],
    resources={
        "spark": create_spark_resource(),
        "s3": create_s3_resource(),
        "streaming_config": create_streaming_config(),
    },
)
