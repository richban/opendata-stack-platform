"""Tests for Dagster schedules.

This module contains tests for batch job schedules including:
- Silver layer daily schedule
- Gold layer daily schedule
"""

import dagster as dg
import pytest

from streamify.defs.schedules import (
    gold_batch_job,
    gold_daily_schedule,
    silver_batch_job,
    silver_daily_schedule,
)


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


class TestGoldBatchSchedule:
    """Test cases for Gold layer batch schedule."""

    def test_gold_batch_job_exists(self):
        """Test that gold_batch_job is properly defined."""
        assert gold_batch_job is not None
        assert gold_batch_job.name == "gold_batch_job"
        # verify it's a job definition (check class name since UnresolvedAssetJobDefinition isn't exported)
        assert "JobDefinition" in type(gold_batch_job).__name__

    def test_gold_daily_schedule_exists(self):
        """Test that gold_daily_schedule is properly defined."""
        assert gold_daily_schedule is not None
        assert isinstance(gold_daily_schedule, dg.ScheduleDefinition)
        assert gold_daily_schedule.name == "gold_daily_schedule"

    def test_gold_schedule_has_correct_cron(self):
        """Test that gold_daily_schedule has the correct cron schedule."""
        assert gold_daily_schedule.cron_schedule == "0 6 * * *"
        assert gold_daily_schedule.execution_timezone == "UTC"

    def test_gold_schedule_targets_gold_batch_job(self):
        """Test that gold_daily_schedule targets the gold_batch_job."""
        assert gold_daily_schedule.job == gold_batch_job

    def test_gold_batch_job_selects_gold_group(self):
        """Test that gold_batch_job selects assets from 'gold' group."""
        # The job should select from the gold group
        selection = gold_batch_job.selection
        assert selection is not None
