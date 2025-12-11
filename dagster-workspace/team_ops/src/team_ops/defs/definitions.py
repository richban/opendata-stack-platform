"""Main Dagster definitions for the Streamify lakehouse pipeline.

This module wires together:
- Assets: Silver layer deduplication, sessionization, and maintenance
- Jobs: Asset jobs for batch processing
- Schedules: Hourly, 4-hourly, and daily schedules
- Resources: Spark, Polaris, MinIO configuration
"""

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)

from team_ops.defs import assets
from team_ops.defs.resources import (
    MinioResource,
    PolarisResource,
    SparkSubmitResource,
)

# Load all assets from the assets module
all_assets = load_assets_from_modules([assets])

# =============================================================================
# Jobs
# =============================================================================

# Silver layer: Deduplication (runs hourly)
silver_dedup_job = define_asset_job(
    name="silver_deduplication_job",
    description="Deduplicate Bronze events to Silver layer",
    selection=AssetSelection.assets(
        assets.silver_listen_events,
        assets.silver_page_view_events,
        assets.silver_auth_events,
    ),
    tags={"layer": "silver", "type": "deduplication"},
)

# Silver layer: Sessionization (runs after deduplication)
silver_sessionize_job = define_asset_job(
    name="silver_sessionization_job",
    description="Build user sessions from Silver events",
    selection=AssetSelection.assets(assets.silver_user_sessions),
    tags={"layer": "silver", "type": "sessionization"},
)

# Full Silver pipeline (dedup + sessionize)
silver_pipeline_job = define_asset_job(
    name="silver_pipeline_job",
    description="Complete Silver layer pipeline: dedup then sessionize",
    selection=AssetSelection.groups("silver"),
    tags={"layer": "silver", "type": "pipeline"},
)

# Maintenance: Compaction (runs every 4 hours)
compaction_job = define_asset_job(
    name="compaction_job",
    description="Compact Iceberg tables",
    selection=AssetSelection.assets(assets.compact_tables),
    tags={"type": "maintenance"},
)

# Maintenance: Snapshot expiry (runs daily)
expire_snapshots_job = define_asset_job(
    name="expire_snapshots_job",
    description="Expire old Iceberg snapshots",
    selection=AssetSelection.assets(assets.expire_snapshots),
    tags={"type": "maintenance"},
)

# =============================================================================
# Schedules
# =============================================================================

# Hourly: Run Silver pipeline
hourly_silver_schedule = ScheduleDefinition(
    name="hourly_silver_pipeline",
    job=silver_pipeline_job,
    cron_schedule="0 * * * *",  # Every hour at minute 0
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
)

# Every 4 hours: Run compaction
four_hourly_compaction_schedule = ScheduleDefinition(
    name="four_hourly_compaction",
    job=compaction_job,
    cron_schedule="0 */4 * * *",  # Every 4 hours at minute 0
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
)

# Daily: Run snapshot expiry at 2 AM UTC
daily_expire_schedule = ScheduleDefinition(
    name="daily_expire_snapshots",
    job=expire_snapshots_job,
    cron_schedule="0 2 * * *",  # Daily at 2 AM UTC
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
)

# =============================================================================
# Definitions
# =============================================================================

defs = Definitions(
    assets=all_assets,
    jobs=[
        silver_dedup_job,
        silver_sessionize_job,
        silver_pipeline_job,
        compaction_job,
        expire_snapshots_job,
    ],
    schedules=[
        hourly_silver_schedule,
        four_hourly_compaction_schedule,
        daily_expire_schedule,
    ],
    resources={
        "spark": SparkSubmitResource(),
        "polaris": PolarisResource(),
        "minio": MinioResource(),
    },
)
