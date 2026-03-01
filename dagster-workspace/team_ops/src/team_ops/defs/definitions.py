"""Dagster definitions for Streamify.

Architecture:
- Streaming assets: Use Dagster Pipes with spark-submit for long-running jobs
- Batch assets: Use Spark Connect for direct PySpark API access

All configuration is managed via ConfigurableResources loaded from environment variables.
"""

import dagster as dg

from team_ops.defs import assets, maintenance_assets, sensors, silver_assets
from team_ops.defs.resources import (
    create_s3_resource,
    create_spark_resource,
    create_streaming_config,
)

# Schedule for daily Bronze table compaction at 2 AM
bronze_compaction_schedule = dg.ScheduleDefinition(
    name="bronze_compaction_schedule",
    target=dg.AssetSelection.assets("bronze_compaction"),
    cron_schedule="0 2 * * *",
    execution_timezone="UTC",
    description="Daily compaction of Bronze Iceberg tables at 2 AM UTC",
)

defs = dg.Definitions(
    assets=dg.load_assets_from_modules([assets, maintenance_assets, silver_assets]),
    sensors=[sensors.bronze_restart_sensor, sensors.kafka_lag_sensor],
    schedules=[bronze_compaction_schedule],
    resources={
        "spark": create_spark_resource(),
        "s3": create_s3_resource(),
        "streaming_config": create_streaming_config(),
    },
)
