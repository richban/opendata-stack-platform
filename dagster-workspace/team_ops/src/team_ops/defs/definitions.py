"""Dagster definitions for Streamify using Spark Connect."""

import dagster as dg

from team_ops.defs import assets
from team_ops.defs.resources import create_spark_resource

defs = dg.Definitions(
    assets=[assets.silver_listen_events],
    resources={"spark": create_spark_resource()},
)
