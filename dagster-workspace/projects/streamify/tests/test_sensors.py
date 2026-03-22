"""Tests for Dagster sensors."""

import json
import time
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
from dagster import (
    DagsterEvent,
    DagsterInstance,
    DagsterRun,
    build_run_status_sensor_context,
    build_sensor_context,
)

from streamify.defs.sensors import (
    _last_restart_timestamps,
    bronze_restart_sensor,
    kafka_lag_sensor,
)


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


class TestKafkaLagSensor:
    """Test cases for kafka_lag_sensor."""

    @pytest.fixture
    def mock_kafka_admin_client(self):
        """Create a mock KafkaAdminClient with sample data."""
        mock_client = MagicMock()

        # Mock consumer groups
        mock_client.list_consumer_groups.return_value = [
            ("spark-streaming-group", "consumer"),
        ]

        # Mock topic description
        mock_client.describe_topics.return_value = [
            {
                "topic": "listen_events",
                "partitions": [
                    {"partition": 0},
                    {"partition": 1},
                ],
            }
        ]

        # Mock committed offsets
        mock_client.list_consumer_group_offsets.return_value = {
            MagicMock(): 100,
            MagicMock(): 200,
        }

        return mock_client

    @pytest.fixture
    def mock_kafka_consumer(self):
        """Create a mock KafkaConsumer with sample end offsets."""
        mock_consumer = MagicMock()
        mock_consumer.end_offsets.return_value = {
            MagicMock(): 500,
            MagicMock(): 800,
        }
        return mock_consumer

    def test_sensor_returns_sensor_result(
        self, mock_kafka_admin_client, mock_kafka_consumer
    ):
        """Test that sensor returns a SensorResult."""
        mock_kafka_module = MagicMock()
        mock_kafka_module.KafkaAdminClient = MagicMock(
            return_value=mock_kafka_admin_client
        )
        mock_kafka_module.KafkaConsumer = MagicMock(return_value=mock_kafka_consumer)

        # Also mock kafka.structs submodule
        mock_structs_module = MagicMock()
        mock_structs_module.TopicPartition = MagicMock()

        with patch.dict(
            "sys.modules",
            {
                "kafka": mock_kafka_module,
                "kafka.structs": mock_structs_module,
            },
        ):
            with patch.dict(
                "os.environ",
                {
                    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                    "KAFKA_LAG_WARN_THRESHOLD": "10000",
                },
            ):
                context = build_sensor_context(
                    sensor_name="kafka_lag_sensor",
                    cursor=json.dumps({"last_checked": time.time() - 120}),
                )

                result = kafka_lag_sensor(context)

                assert isinstance(result, dg.SensorResult)
                assert result.cursor is not None
                cursor_data = json.loads(result.cursor)
                assert "last_checked" in cursor_data
                assert "topics_checked" in cursor_data

    def test_sensor_handles_kafka_error(self, mock_kafka_admin_client):
        """Test that sensor handles Kafka connection errors gracefully."""
        mock_kafka_admin_client.list_consumer_groups.side_effect = Exception(
            "Connection refused"
        )

        mock_kafka_module = MagicMock()
        mock_kafka_module.KafkaAdminClient = MagicMock(
            return_value=mock_kafka_admin_client
        )
        mock_kafka_module.KafkaConsumer = MagicMock()

        # Also mock kafka.structs submodule
        mock_structs_module = MagicMock()
        mock_structs_module.TopicPartition = MagicMock()

        with patch.dict(
            "sys.modules",
            {
                "kafka": mock_kafka_module,
                "kafka.structs": mock_structs_module,
            },
        ):
            with patch.dict(
                "os.environ",
                {
                    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                    "KAFKA_LAG_WARN_THRESHOLD": "10000",
                },
            ):
                context = build_sensor_context(sensor_name="kafka_lag_sensor")

                result = kafka_lag_sensor(context)

                assert isinstance(result, dg.SensorResult)
                assert result.cursor is not None
                cursor_data = json.loads(result.cursor)
                assert "error" in cursor_data
