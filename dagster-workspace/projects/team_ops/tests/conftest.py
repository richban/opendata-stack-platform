"""Test fixtures for team_ops project."""

from unittest.mock import MagicMock

import pytest

from pyspark.sql import SparkSession
from team_ops.defs.resources import StreamingJobConfig


@pytest.fixture
def mock_spark_resource():
    """Mock SparkSession direct instance for testing."""
    return MagicMock(spec=SparkSession)


@pytest.fixture
def mock_s3_resource():
    """Mock S3Resource for testing."""
    mock_s3 = MagicMock()
    mock_s3.get_client.return_value = MagicMock()
    return mock_s3


@pytest.fixture
def mock_streaming_config():
    """StreamingJobConfig with test values."""
    return StreamingJobConfig(
        kafka_bootstrap_servers="kafka:9092",
        checkpoint_path="s3a://checkpoints/streaming",
        polaris_uri="http://polaris:8181",
        polaris_client_id="test-client-id",
        polaris_client_secret="test-client-secret",
        catalog="streamify",
        namespace="bronze",
        dagster_pipes_bucket="dagster-pipes",
    )


@pytest.fixture
def mock_spark_session():
    """Create a mock SparkSession with streaming capabilities."""
    mock_session = MagicMock()

    # Mock SQL execution
    mock_session.sql.return_value = MagicMock()

    # Mock streams
    mock_streams = MagicMock()
    mock_streams.awaitAnyTermination.return_value = None
    mock_session.streams = mock_streams

    # Mock DataFrame operations for streaming
    mock_df = MagicMock()
    mock_df.writeStream = MagicMock()
    mock_write_stream = MagicMock()
    mock_write_stream.format.return_value = mock_write_stream
    mock_write_stream.outputMode.return_value = mock_write_stream
    mock_write_stream.trigger.return_value = mock_write_stream
    mock_write_stream.option.return_value = mock_write_stream
    mock_write_stream.toTable.return_value = MagicMock()
    mock_df.writeStream = mock_write_stream

    mock_read_stream = MagicMock()
    mock_read_stream.format.return_value = mock_read_stream
    mock_read_stream.option.return_value = mock_read_stream
    mock_read_stream.load.return_value = mock_df
    mock_session.readStream = mock_read_stream

    return mock_session
