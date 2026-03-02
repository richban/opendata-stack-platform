"""Dagster schedules for batch asset processing.

This module contains scheduled jobs for running Silver and Gold layer
assets in dependency order on a regular cadence.
"""

from __future__ import annotations

import dagster as dg

# Silver batch job - materializes all Silver assets in dependency order
silver_batch_job = dg.define_asset_job(
    name="silver_batch_job",
    selection=dg.AssetSelection.groups("streamify").downstream(),
    description="Materializes all Silver layer assets in dependency order",
)


# Silver daily schedule - runs at 4 AM UTC daily
silver_daily_schedule = dg.ScheduleDefinition(
    name="silver_daily_schedule",
    job=silver_batch_job,
    cron_schedule="0 4 * * *",
    execution_timezone="UTC",
    description="Daily materialization of all Silver layer assets at 4 AM UTC",
)


# Gold batch job - materializes all Gold assets
gold_batch_job = dg.define_asset_job(
    name="gold_batch_job",
    selection=dg.AssetSelection.groups("gold"),
    description="Materializes all Gold layer assets",
)


# Gold daily schedule - runs at 6 AM UTC daily (2 hours after Silver)
gold_daily_schedule = dg.ScheduleDefinition(
    name="gold_daily_schedule",
    job=gold_batch_job,
    cron_schedule="0 6 * * *",
    execution_timezone="UTC",
    description=(
        "Daily materialization of all Gold layer assets at 6 AM UTC "
        "(2 hours after Silver)"
    ),
)
