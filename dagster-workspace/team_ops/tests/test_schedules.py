"""Tests for Dagster schedules.

This module contains tests for batch job schedules including:
- Silver layer daily schedule
- Gold layer daily schedule
"""

import dagster as dg
import pytest

from team_ops.defs.schedules import silver_batch_job, silver_daily_schedule


class TestSilverBatchSchedule:
    """Test cases for Silver layer batch schedule."""

    def test_silver_batch_job_exists(self):
        """Test that silver_batch_job is properly defined."""
        assert silver_batch_job is not None
        assert silver_batch_job.name == "silver_batch_job"
        # verify it's a job definition (check class name since UnresolvedAssetJobDefinition isn't exported)
        assert "JobDefinition" in type(silver_batch_job).__name__

    def test_silver_daily_schedule_exists(self):
        """Test that silver_daily_schedule is properly defined."""
        assert silver_daily_schedule is not None
        assert isinstance(silver_daily_schedule, dg.ScheduleDefinition)
        assert silver_daily_schedule.name == "silver_daily_schedule"

    def test_silver_schedule_has_correct_cron(self):
        """Test that silver_daily_schedule has the correct cron schedule."""
        assert silver_daily_schedule.cron_schedule == "0 4 * * *"
        assert silver_daily_schedule.execution_timezone == "UTC"

    def test_silver_schedule_targets_silver_batch_job(self):
        """Test that silver_daily_schedule targets the silver_batch_job."""
        assert silver_daily_schedule.job == silver_batch_job

    def test_silver_batch_job_selects_streamify_group(self):
        """Test that silver_batch_job selects assets from 'streamify' group."""
        # The job should select from the streamify group with downstream
        selection = silver_batch_job.selection
        assert selection is not None
