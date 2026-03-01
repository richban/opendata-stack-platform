"""Tests for Dagster sensors."""

import time
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
from dagster import (
    DagsterEvent,
    DagsterInstance,
    DagsterRun,
    build_run_status_sensor_context,
)

from team_ops.defs.sensors import _last_restart_timestamps, bronze_restart_sensor


@pytest.fixture(autouse=True)
def clear_restart_timestamps():
    """Clear the restart timestamps before each test."""
    _last_restart_timestamps.clear()
    yield
    _last_restart_timestamps.clear()


@pytest.fixture
def mock_failure_context():
    """Create mock objects for testing run failure sensor."""
    # Create a mock run
    mock_run = MagicMock(spec=DagsterRun)
    mock_run.run_id = "test-run-id-123"
    mock_run.job_name = "bronze_streaming_job"

    # Create a mock failure event (DagsterEvent)
    mock_failure_event = MagicMock(spec=DagsterEvent)
    mock_failure_event.message = "Test failure: connection lost to Kafka"

    # Create a mock instance
    mock_instance = MagicMock(spec=DagsterInstance)

    return mock_run, mock_failure_event, mock_instance


class TestBronzeRestartSensor:
    """Test cases for bronze_restart_sensor."""

    def test_sensor_yields_run_request_on_first_failure(self, mock_failure_context):
        """Test that sensor yields RunRequest on first failure."""
        mock_run, mock_failure_event, mock_instance = mock_failure_context

        with patch.dict("os.environ", {"STREAMING_RESTART_MIN_INTERVAL_SECONDS": "60"}):
            context = build_run_status_sensor_context(
                sensor_name="bronze_restart_sensor",
                dagster_run=mock_run,
                dagster_event=mock_failure_event,
                dagster_instance=mock_instance,
            ).for_run_failure()

            result = bronze_restart_sensor(context)

            assert isinstance(result, dg.RunRequest)
            assert result.run_key is not None
            assert result.run_key.startswith("bronze_restart_")
            assert result.tags["triggered_by"] == "bronze_restart_sensor"
            assert result.tags["failure_run_id"] == "test-run-id-123"

    def test_sensor_yields_run_request_after_cooldown_elapsed(self, mock_failure_context):
        """Test that sensor yields RunRequest when cooldown has elapsed."""
        mock_run, mock_failure_event, mock_instance = mock_failure_context

        # Set last restart to 120 seconds ago (cooldown is 60s)
        _last_restart_timestamps["bronze_streaming_job"] = time.time() - 120

        with patch.dict("os.environ", {"STREAMING_RESTART_MIN_INTERVAL_SECONDS": "60"}):
            context = build_run_status_sensor_context(
                sensor_name="bronze_restart_sensor",
                dagster_run=mock_run,
                dagster_event=mock_failure_event,
                dagster_instance=mock_instance,
            ).for_run_failure()

            result = bronze_restart_sensor(context)

            assert isinstance(result, dg.RunRequest)
            assert result.run_key is not None
            assert result.run_key.startswith("bronze_restart_")

    def test_sensor_skips_when_cooldown_active(self, mock_failure_context):
        """Test that sensor returns SkipReason when cooldown hasn't elapsed."""
        mock_run, mock_failure_event, mock_instance = mock_failure_context

        # Set last restart to 10 seconds ago (cooldown is 60s, so only 10s elapsed)
        _last_restart_timestamps["bronze_streaming_job"] = time.time() - 10

        with patch.dict("os.environ", {"STREAMING_RESTART_MIN_INTERVAL_SECONDS": "60"}):
            context = build_run_status_sensor_context(
                sensor_name="bronze_restart_sensor",
                dagster_run=mock_run,
                dagster_event=mock_failure_event,
                dagster_instance=mock_instance,
            ).for_run_failure()

            result = bronze_restart_sensor(context)

            assert isinstance(result, dg.SkipReason)
            assert "Cooldown active" in str(result)

    def test_sensor_uses_custom_restart_interval(self, mock_failure_context):
        """Test that sensor respects custom STREAMING_RESTART_MIN_INTERVAL_SECONDS."""
        mock_run, mock_failure_event, mock_instance = mock_failure_context

        # Set last restart to 150 seconds ago, but custom interval is 300s
        _last_restart_timestamps["bronze_streaming_job"] = time.time() - 150

        with patch.dict("os.environ", {"STREAMING_RESTART_MIN_INTERVAL_SECONDS": "300"}):
            context = build_run_status_sensor_context(
                sensor_name="bronze_restart_sensor",
                dagster_run=mock_run,
                dagster_event=mock_failure_event,
                dagster_instance=mock_instance,
            ).for_run_failure()

            result = bronze_restart_sensor(context)

            # Should skip because 150s < 300s
            assert isinstance(result, dg.SkipReason)
            assert "Cooldown active" in str(result)

    def test_sensor_logs_failure_message(self, mock_failure_context, capsys):
        """Test that sensor logs the failure event message to stderr."""
        mock_run, mock_failure_event, mock_instance = mock_failure_context

        with patch.dict("os.environ", {"STREAMING_RESTART_MIN_INTERVAL_SECONDS": "60"}):
            context = build_run_status_sensor_context(
                sensor_name="bronze_restart_sensor",
                dagster_run=mock_run,
                dagster_event=mock_failure_event,
                dagster_instance=mock_instance,
            ).for_run_failure()

            bronze_restart_sensor(context)

            # Check that failure message was logged to stderr (Dagster's logger behavior)
            captured = capsys.readouterr()
            assert "bronze_streaming_job failed" in captured.err
            assert "connection lost to Kafka" in captured.err

    def test_sensor_skips_non_bronze_jobs(self, mock_failure_context):
        """Test that sensor skips failures from other jobs."""
        mock_run, mock_failure_event, mock_instance = mock_failure_context

        # Change job name to something else
        mock_run.job_name = "other_job"

        with patch.dict("os.environ", {"STREAMING_RESTART_MIN_INTERVAL_SECONDS": "60"}):
            context = build_run_status_sensor_context(
                sensor_name="bronze_restart_sensor",
                dagster_run=mock_run,
                dagster_event=mock_failure_event,
                dagster_instance=mock_instance,
            ).for_run_failure()

            result = bronze_restart_sensor(context)

            assert isinstance(result, dg.SkipReason)
            assert "Skipping non-bronze job" in str(result)
            assert "other_job" in str(result)

    def test_sensor_updates_timestamp_on_restart(self, mock_failure_context):
        """Test that sensor updates last restart timestamp when triggering restart."""
        mock_run, mock_failure_event, mock_instance = mock_failure_context

        # Ensure no previous timestamp
        assert "bronze_streaming_job" not in _last_restart_timestamps

        with patch.dict("os.environ", {"STREAMING_RESTART_MIN_INTERVAL_SECONDS": "60"}):
            context = build_run_status_sensor_context(
                sensor_name="bronze_restart_sensor",
                dagster_run=mock_run,
                dagster_event=mock_failure_event,
                dagster_instance=mock_instance,
            ).for_run_failure()

            before_call = time.time()
            bronze_restart_sensor(context)
            after_call = time.time()

            # Verify timestamp was set
            assert "bronze_streaming_job" in _last_restart_timestamps
            timestamp = _last_restart_timestamps["bronze_streaming_job"]
            assert before_call <= timestamp <= after_call
