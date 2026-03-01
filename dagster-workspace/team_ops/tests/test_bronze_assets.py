"""Tests for Bronze layer streaming assets."""

from unittest.mock import MagicMock, patch

import dagster as dg
from dagster import build_op_context

from team_ops.defs.assets import bronze_streaming_job
from team_ops.defs.definitions import defs
from team_ops.defs.resources import StreamingJobConfig


def test_definitions_load_without_errors():
    """Verify that definitions load without errors."""
    # If defs object is created successfully, no load errors occurred
    assert defs is not None
    assert defs.assets is not None


def test_bronze_streaming_job_asset_exists():
    """Verify bronze_streaming_job asset is defined and has correct metadata."""
    # Check that the asset exists
    assert bronze_streaming_job is not None

    # Check the asset key
    assert bronze_streaming_job.key == dg.AssetKey(["bronze_streaming_job"])

    # Verify asset has correct keys and properties
    # These are set in the decorator
    assert len(bronze_streaming_job.keys) == 1


@patch("team_ops.defs.assets.col")
@patch("team_ops.defs.assets.from_json")
@patch("team_ops.defs.assets.concat_ws")
@patch("team_ops.defs.assets.sha2")
@patch("team_ops.defs.assets.to_date")
@patch("team_ops.defs.assets.from_unixtime")
@patch("team_ops.defs.assets.current_timestamp")
def test_bronze_streaming_job_asset_function(
    mock_current_timestamp,
    mock_from_unixtime,
    mock_to_date,
    mock_sha2,
    mock_concat_ws,
    mock_from_json,
    mock_col,
):
    """Test the bronze_streaming_job function directly with mocked dependencies."""
    # Create mock resources
    mock_spark_session = MagicMock()
    mock_spark_resource = MagicMock()
    mock_spark_resource.get_session.return_value = mock_spark_session

    # Use a real StreamingJobConfig with test values
    streaming_config = StreamingJobConfig(
        kafka_bootstrap_servers="kafka:9092",
        checkpoint_path="s3a://checkpoints/streaming",
        polaris_uri="http://polaris:8181",
        polaris_client_id="test-client-id",
        polaris_client_secret="test-client-secret",
        catalog="streamify",
        namespace="bronze",
        dagster_pipes_bucket="dagster-pipes",
    )

    # Mock the awaitAnyTermination to prevent blocking
    mock_spark_session.streams.awaitAnyTermination = MagicMock()

    # Mock SQL execution (for CREATE NAMESPACE and DESCRIBE TABLE)
    mock_spark_session.sql.return_value = MagicMock()

    # Mock DataFrame operations for streaming
    mock_df = MagicMock()

    # Setup readStream chain
    mock_read_stream = MagicMock()
    mock_read_stream.format.return_value = mock_read_stream
    mock_read_stream.option.return_value = mock_read_stream
    mock_read_stream.load.return_value = mock_df
    mock_spark_session.readStream = mock_read_stream

    # Setup writeStream chain
    mock_write_stream = MagicMock()
    mock_write_stream.format.return_value = mock_write_stream
    mock_write_stream.outputMode.return_value = mock_write_stream
    mock_write_stream.trigger.return_value = mock_write_stream
    mock_write_stream.option.return_value = mock_write_stream
    mock_write_stream.toTable.return_value = MagicMock()
    mock_df.writeStream = mock_write_stream

    # Mock the select operations
    mock_df.select.return_value = mock_df
    mock_df.withColumn.return_value = mock_df

    # Mock the PySpark functions
    mock_col.return_value = MagicMock()
    mock_from_json.return_value = MagicMock()
    mock_concat_ws.return_value = MagicMock()
    mock_sha2.return_value = MagicMock()
    mock_to_date.return_value = MagicMock()
    mock_from_unixtime.return_value = MagicMock()
    mock_current_timestamp.return_value = MagicMock()

    # Build the context with resources
    context = build_op_context(
        resources={
            "spark": mock_spark_resource,
            "streaming_config": streaming_config,
        }
    )

    # Call the asset function
    result_gen = bronze_streaming_job(context)

    # Get the MaterializeResult from the generator
    result = next(result_gen)

    # Verify the result
    assert result is not None
    assert isinstance(result, dg.MaterializeResult)

    # Verify get_session was called
    mock_spark_resource.get_session.assert_called_once()

    # Verify SQL was called for namespace creation
    assert mock_spark_session.sql.call_count >= 1

    # Verify streaming query was started for each topic (3 topics)
    assert mock_write_stream.toTable.call_count == 3


@patch("team_ops.defs.assets.col")
@patch("team_ops.defs.assets.from_json")
@patch("team_ops.defs.assets.concat_ws")
@patch("team_ops.defs.assets.sha2")
@patch("team_ops.defs.assets.to_date")
@patch("team_ops.defs.assets.from_unixtime")
@patch("team_ops.defs.assets.current_timestamp")
def test_bronze_streaming_job_metadata_content(
    mock_current_timestamp,
    mock_from_unixtime,
    mock_to_date,
    mock_sha2,
    mock_concat_ws,
    mock_from_json,
    mock_col,
):
    """Test that the asset returns expected metadata fields."""
    # Create mock resources
    mock_spark_session = MagicMock()
    mock_spark_resource = MagicMock()
    mock_spark_resource.get_session.return_value = mock_spark_session

    # Use a real StreamingJobConfig with test values
    streaming_config = StreamingJobConfig(
        kafka_bootstrap_servers="kafka:9092",
        checkpoint_path="s3a://checkpoints/streaming",
        polaris_uri="http://polaris:8181",
        polaris_client_id="test-client-id",
        polaris_client_secret="test-client-secret",
        catalog="streamify",
        namespace="bronze",
        dagster_pipes_bucket="dagster-pipes",
    )

    # Mock streaming operations
    mock_spark_session.streams.awaitAnyTermination = MagicMock()
    mock_spark_session.sql.return_value = MagicMock()

    # Mock DataFrame chain
    mock_df = MagicMock()
    mock_df.select.return_value = mock_df
    mock_df.withColumn.return_value = mock_df

    mock_read_stream = MagicMock()
    mock_read_stream.format.return_value = mock_read_stream
    mock_read_stream.option.return_value = mock_read_stream
    mock_read_stream.load.return_value = mock_df
    mock_spark_session.readStream = mock_read_stream

    mock_write_stream = MagicMock()
    mock_write_stream.format.return_value = mock_write_stream
    mock_write_stream.outputMode.return_value = mock_write_stream
    mock_write_stream.trigger.return_value = mock_write_stream
    mock_write_stream.option.return_value = mock_write_stream
    mock_write_stream.toTable.return_value = MagicMock()
    mock_df.writeStream = mock_write_stream

    # Mock the PySpark functions
    mock_col.return_value = MagicMock()
    mock_from_json.return_value = MagicMock()
    mock_concat_ws.return_value = MagicMock()
    mock_sha2.return_value = MagicMock()
    mock_to_date.return_value = MagicMock()
    mock_from_unixtime.return_value = MagicMock()
    mock_current_timestamp.return_value = MagicMock()

    # Build the context with resources
    context = build_op_context(
        resources={
            "spark": mock_spark_resource,
            "streaming_config": streaming_config,
        }
    )

    # Call the asset function
    result_gen = bronze_streaming_job(context)
    result = next(result_gen)

    # Verify metadata contains expected fields (new US-002 fields)
    metadata = result.metadata
    assert "topics_started" in metadata
    assert "spark_ui_url" in metadata
    assert "catalog" in metadata
    assert "checkpoint_base" in metadata
    # Also check backward-compatible fields
    assert "topics" in metadata
    assert "namespace" in metadata
    assert "kafka_servers" in metadata
    assert "num_streams" in metadata

    # Verify metadata values (access .text/.value attributes for MetadataValue objects)
    assert metadata["catalog"].text == "streamify"
    assert metadata["checkpoint_base"].text == "s3a://checkpoints/streaming"
    assert metadata["namespace"] == "bronze"
    assert metadata["kafka_servers"] == "kafka:9092"
    assert metadata["num_streams"] == 3  # Three topics
