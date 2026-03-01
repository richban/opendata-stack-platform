"""Tests for Dagster Silver layer assets."""

from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
from dagster import build_op_context

from team_ops.defs.resources import SparkConnectResource, StreamingJobConfig
from team_ops.defs.silver_assets import (
    silver_auth_events,
    silver_listen_events,
    silver_page_view_events,
)


class TestSilverListenEventsAsset:
    """Test cases for silver_listen_events asset."""

    @patch("team_ops.defs.silver_assets.col")
    @patch("team_ops.defs.silver_assets.row_number")
    @patch("team_ops.defs.silver_assets.Window")
    @patch.object(SparkConnectResource, "get_session")
    def test_asset_deduplicates_rows_correctly(
        self, mock_get_session, mock_Window, mock_row_number, mock_col
    ):
        """Test that asset deduplicates rows using ROW_NUMBER window function."""
        # Setup mock Spark session
        mock_session = MagicMock()

        # Create mock DataFrame that simulates input data with duplicates
        mock_df = MagicMock()
        mock_df.count.return_value = 100  # Input rows

        # Build the chain: df -> withColumn -> filter -> drop
        # Each step returns a new mock to avoid confusion
        mock_with_row_num = MagicMock()
        mock_after_filter = MagicMock()
        mock_final_df = MagicMock()

        # Setup count on the FINAL df (after drop)
        mock_final_df.count.return_value = 95

        # Setup write chain on the final df
        mock_writer = MagicMock()
        mock_final_df.write = mock_writer
        mock_mode_writer = MagicMock()
        mock_writer.mode.return_value = mock_mode_writer
        mock_option_writer = MagicMock()
        mock_mode_writer.option.return_value = mock_option_writer
        mock_partition_writer = MagicMock()
        mock_option_writer.partitionBy.return_value = mock_partition_writer
        mock_format_writer = MagicMock()
        mock_partition_writer.format.return_value = mock_format_writer
        mock_format_writer.saveAsTable.return_value = None

        # Build chain: df.withColumn(...).filter(...).drop(...)
        mock_df.withColumn.return_value = mock_with_row_num
        mock_with_row_num.filter.return_value = mock_after_filter
        mock_after_filter.drop.return_value = mock_final_df

        mock_session.table.return_value = mock_df
        mock_get_session.return_value = mock_session

        # Mock PySpark functions
        mock_col.return_value = MagicMock()
        mock_col.return_value.desc.return_value = MagicMock()
        mock_row_number.return_value = MagicMock()
        mock_Window.partitionBy.return_value.orderBy.return_value = MagicMock()

        # Create real resources
        spark_resource = SparkConnectResource(
            spark_remote="sc://localhost:15002",
            polaris_client_id="test-client-id",
            polaris_client_secret="test-client-secret",
            polaris_uri="http://localhost:8181",
            catalog="streamify",
        )

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

        # Build context without partition (full table mode)
        context = build_op_context(
            resources={
                "spark": spark_resource,
                "streaming_config": streaming_config,
            }
        )

        # Execute asset
        result = silver_listen_events(context)

        # Verify result type
        assert isinstance(result, dg.MaterializeResult)
        assert result.metadata is not None

        # Verify metadata
        assert result.metadata["input_rows"].value == 100
        assert result.metadata["output_rows"].value == 95
        assert result.metadata["duplicate_rows_removed"].value == 5

        # Verify Window.partitionBy was called with event_id
        mock_Window.partitionBy.assert_called_once_with("event_id")

    @patch("team_ops.defs.silver_assets.col")
    @patch("team_ops.defs.silver_assets.row_number")
    @patch("team_ops.defs.silver_assets.Window")
    @patch.object(SparkConnectResource, "get_session")
    def test_asset_returns_materialize_result(
        self, mock_get_session, mock_Window, mock_row_number, mock_col
    ):
        """Test that asset returns MaterializeResult with expected metadata."""
        # Setup mock Spark session
        mock_session = MagicMock()

        # Create mock DataFrame chain
        mock_df = MagicMock()
        mock_df.count.return_value = 50

        mock_deduped_df = MagicMock()
        mock_deduped_df.count.return_value = 48
        mock_deduped_df.drop.return_value = mock_deduped_df

        mock_df.withColumn.return_value.filter.return_value = mock_deduped_df

        # Mock write chain
        mock_writer = MagicMock()
        mock_deduped_df.write = mock_writer
        mock_writer.mode.return_value.option.return_value.partitionBy.return_value.format.return_value.saveAsTable.return_value = None

        mock_session.table.return_value = mock_df
        mock_get_session.return_value = mock_session

        # Mock PySpark functions
        mock_col.return_value = MagicMock()
        mock_col.return_value.desc.return_value = MagicMock()
        mock_row_number.return_value = MagicMock()
        mock_Window.partitionBy.return_value.orderBy.return_value = MagicMock()

        # Create real resources
        spark_resource = SparkConnectResource(
            spark_remote="sc://localhost:15002",
            polaris_client_id="test-client-id",
            polaris_client_secret="test-client-secret",
            polaris_uri="http://localhost:8181",
            catalog="streamify",
        )

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

        # Build context
        context = build_op_context(
            resources={
                "spark": spark_resource,
                "streaming_config": streaming_config,
            }
        )

        # Execute asset
        result = silver_listen_events(context)

        # Verify metadata structure
        assert "input_rows" in result.metadata
        assert "output_rows" in result.metadata
        assert "duplicate_rows_removed" in result.metadata
        assert "source_table" in result.metadata
        assert "target_table" in result.metadata
        assert "partition_date" in result.metadata

        # Verify table names
        assert "bronze_listen_events" in result.metadata["source_table"].text
        assert "silver_listen_events" in result.metadata["target_table"].text


class TestSilverPageViewEventsAsset:
    """Test cases for silver_page_view_events asset."""

    @patch("team_ops.defs.silver_assets.col")
    @patch("team_ops.defs.silver_assets.row_number")
    @patch("team_ops.defs.silver_assets.Window")
    @patch.object(SparkConnectResource, "get_session")
    def test_asset_deduplicates_rows_correctly(
        self, mock_get_session, mock_Window, mock_row_number, mock_col
    ):
        """Test that asset deduplicates rows using ROW_NUMBER window function."""
        # Setup mock Spark session
        mock_session = MagicMock()

        # Create mock DataFrame that simulates input data with duplicates
        mock_df = MagicMock()
        mock_df.count.return_value = 200  # Input rows

        # Build the chain: df -> withColumn -> filter -> drop
        # Each step returns a new mock to avoid confusion
        mock_with_row_num = MagicMock()
        mock_after_filter = MagicMock()
        mock_final_df = MagicMock()

        # Setup count on the FINAL df (after drop)
        mock_final_df.count.return_value = 190

        # Setup write chain on the final df
        mock_writer = MagicMock()
        mock_final_df.write = mock_writer
        mock_mode_writer = MagicMock()
        mock_writer.mode.return_value = mock_mode_writer
        mock_option_writer = MagicMock()
        mock_mode_writer.option.return_value = mock_option_writer
        mock_partition_writer = MagicMock()
        mock_option_writer.partitionBy.return_value = mock_partition_writer
        mock_format_writer = MagicMock()
        mock_partition_writer.format.return_value = mock_format_writer
        mock_format_writer.saveAsTable.return_value = None

        # Build chain: df.withColumn(...).filter(...).drop(...)
        mock_df.withColumn.return_value = mock_with_row_num
        mock_with_row_num.filter.return_value = mock_after_filter
        mock_after_filter.drop.return_value = mock_final_df

        mock_session.table.return_value = mock_df
        mock_get_session.return_value = mock_session

        # Mock PySpark functions
        mock_col.return_value = MagicMock()
        mock_col.return_value.desc.return_value = MagicMock()
        mock_row_number.return_value = MagicMock()
        mock_Window.partitionBy.return_value.orderBy.return_value = MagicMock()

        # Create real resources
        spark_resource = SparkConnectResource(
            spark_remote="sc://localhost:15002",
            polaris_client_id="test-client-id",
            polaris_client_secret="test-client-secret",
            polaris_uri="http://localhost:8181",
            catalog="streamify",
        )

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

        # Build context without partition (full table mode)
        context = build_op_context(
            resources={
                "spark": spark_resource,
                "streaming_config": streaming_config,
            }
        )

        # Execute asset
        result = silver_page_view_events(context)

        # Verify result type
        assert isinstance(result, dg.MaterializeResult)
        assert result.metadata is not None

        # Verify metadata
        assert result.metadata["input_rows"].value == 200
        assert result.metadata["output_rows"].value == 190
        assert result.metadata["duplicate_rows_removed"].value == 10

        # Verify Window.partitionBy was called with event_id
        mock_Window.partitionBy.assert_called_once_with("event_id")

    @patch("team_ops.defs.silver_assets.col")
    @patch("team_ops.defs.silver_assets.row_number")
    @patch("team_ops.defs.silver_assets.Window")
    @patch.object(SparkConnectResource, "get_session")
    def test_asset_returns_materialize_result(
        self, mock_get_session, mock_Window, mock_row_number, mock_col
    ):
        """Test that asset returns MaterializeResult with expected metadata."""
        # Setup mock Spark session
        mock_session = MagicMock()

        # Create mock DataFrame chain
        mock_df = MagicMock()
        mock_df.count.return_value = 75

        mock_deduped_df = MagicMock()
        mock_deduped_df.count.return_value = 73
        mock_deduped_df.drop.return_value = mock_deduped_df

        mock_df.withColumn.return_value.filter.return_value = mock_deduped_df

        # Mock write chain
        mock_writer = MagicMock()
        mock_deduped_df.write = mock_writer
        mock_writer.mode.return_value.option.return_value.partitionBy.return_value.format.return_value.saveAsTable.return_value = None

        mock_session.table.return_value = mock_df
        mock_get_session.return_value = mock_session

        # Mock PySpark functions
        mock_col.return_value = MagicMock()
        mock_col.return_value.desc.return_value = MagicMock()
        mock_row_number.return_value = MagicMock()
        mock_Window.partitionBy.return_value.orderBy.return_value = MagicMock()

        # Create real resources
        spark_resource = SparkConnectResource(
            spark_remote="sc://localhost:15002",
            polaris_client_id="test-client-id",
            polaris_client_secret="test-client-secret",
            polaris_uri="http://localhost:8181",
            catalog="streamify",
        )

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

        # Build context
        context = build_op_context(
            resources={
                "spark": spark_resource,
                "streaming_config": streaming_config,
            }
        )

        # Execute asset
        result = silver_page_view_events(context)

        # Verify metadata structure
        assert "input_rows" in result.metadata
        assert "output_rows" in result.metadata
        assert "duplicate_rows_removed" in result.metadata
        assert "source_table" in result.metadata
        assert "target_table" in result.metadata
        assert "partition_date" in result.metadata

        # Verify table names
        assert "bronze_page_view_events" in result.metadata["source_table"].text
        assert "silver_page_view_events" in result.metadata["target_table"].text


class TestSilverAuthEventsAsset:
    """Test cases for silver_auth_events asset."""

    @patch("team_ops.defs.silver_assets.col")
    @patch("team_ops.defs.silver_assets.row_number")
    @patch("team_ops.defs.silver_assets.Window")
    @patch.object(SparkConnectResource, "get_session")
    def test_asset_deduplicates_rows_correctly(
        self, mock_get_session, mock_Window, mock_row_number, mock_col
    ):
        """Test that asset deduplicates rows using ROW_NUMBER window function."""
        # Setup mock Spark session
        mock_session = MagicMock()

        # Create mock DataFrame that simulates input data with duplicates
        mock_df = MagicMock()
        mock_df.count.return_value = 150  # Input rows

        # Build the chain: df -> withColumn -> filter -> drop
        # Each step returns a new mock to avoid confusion
        mock_with_row_num = MagicMock()
        mock_after_filter = MagicMock()
        mock_final_df = MagicMock()

        # Setup count on the FINAL df (after drop)
        mock_final_df.count.return_value = 145

        # Setup write chain on the final df
        mock_writer = MagicMock()
        mock_final_df.write = mock_writer
        mock_mode_writer = MagicMock()
        mock_writer.mode.return_value = mock_mode_writer
        mock_option_writer = MagicMock()
        mock_mode_writer.option.return_value = mock_option_writer
        mock_partition_writer = MagicMock()
        mock_option_writer.partitionBy.return_value = mock_partition_writer
        mock_format_writer = MagicMock()
        mock_partition_writer.format.return_value = mock_format_writer
        mock_format_writer.saveAsTable.return_value = None

        # Build chain: df.withColumn(...).filter(...).drop(...)
        mock_df.withColumn.return_value = mock_with_row_num
        mock_with_row_num.filter.return_value = mock_after_filter
        mock_after_filter.drop.return_value = mock_final_df

        mock_session.table.return_value = mock_df
        mock_get_session.return_value = mock_session

        # Mock PySpark functions
        mock_col.return_value = MagicMock()
        mock_col.return_value.desc.return_value = MagicMock()
        mock_row_number.return_value = MagicMock()
        mock_Window.partitionBy.return_value.orderBy.return_value = MagicMock()

        # Create real resources
        spark_resource = SparkConnectResource(
            spark_remote="sc://localhost:15002",
            polaris_client_id="test-client-id",
            polaris_client_secret="test-client-secret",
            polaris_uri="http://localhost:8181",
            catalog="streamify",
        )

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

        # Build context without partition (full table mode)
        context = build_op_context(
            resources={
                "spark": spark_resource,
                "streaming_config": streaming_config,
            }
        )

        # Execute asset
        result = silver_auth_events(context)

        # Verify result type
        assert isinstance(result, dg.MaterializeResult)
        assert result.metadata is not None

        # Verify metadata
        assert result.metadata["input_rows"].value == 150
        assert result.metadata["output_rows"].value == 145
        assert result.metadata["duplicate_rows_removed"].value == 5

        # Verify Window.partitionBy was called with event_id
        mock_Window.partitionBy.assert_called_once_with("event_id")

    @patch("team_ops.defs.silver_assets.col")
    @patch("team_ops.defs.silver_assets.row_number")
    @patch("team_ops.defs.silver_assets.Window")
    @patch.object(SparkConnectResource, "get_session")
    def test_asset_returns_materialize_result(
        self, mock_get_session, mock_Window, mock_row_number, mock_col
    ):
        """Test that asset returns MaterializeResult with expected metadata."""
        # Setup mock Spark session
        mock_session = MagicMock()

        # Create mock DataFrame chain
        mock_df = MagicMock()
        mock_df.count.return_value = 80

        mock_deduped_df = MagicMock()
        mock_deduped_df.count.return_value = 78
        mock_deduped_df.drop.return_value = mock_deduped_df

        mock_df.withColumn.return_value.filter.return_value = mock_deduped_df

        # Mock write chain
        mock_writer = MagicMock()
        mock_deduped_df.write = mock_writer
        mock_writer.mode.return_value.option.return_value.partitionBy.return_value.format.return_value.saveAsTable.return_value = None

        mock_session.table.return_value = mock_df
        mock_get_session.return_value = mock_session

        # Mock PySpark functions
        mock_col.return_value = MagicMock()
        mock_col.return_value.desc.return_value = MagicMock()
        mock_row_number.return_value = MagicMock()
        mock_Window.partitionBy.return_value.orderBy.return_value = MagicMock()

        # Create real resources
        spark_resource = SparkConnectResource(
            spark_remote="sc://localhost:15002",
            polaris_client_id="test-client-id",
            polaris_client_secret="test-client-secret",
            polaris_uri="http://localhost:8181",
            catalog="streamify",
        )

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

        # Build context
        context = build_op_context(
            resources={
                "spark": spark_resource,
                "streaming_config": streaming_config,
            }
        )

        # Execute asset
        result = silver_auth_events(context)

        # Verify metadata structure
        assert "input_rows" in result.metadata
        assert "output_rows" in result.metadata
        assert "duplicate_rows_removed" in result.metadata
        assert "source_table" in result.metadata
        assert "target_table" in result.metadata
        assert "partition_date" in result.metadata

        # Verify table names
        assert "bronze_auth_events" in result.metadata["source_table"].text
        assert "silver_auth_events" in result.metadata["target_table"].text
