"""Tests for Dagster Silver layer assets."""

from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
from dagster import build_op_context

from streamify.defs.resources import StreamingJobConfig
from streamify.defs.silver_assets import (
    silver_auth_events,
    silver_listen_events,
    silver_page_view_events,
    silver_user_sessions,
)


class TestSilverListenEventsAsset:
    """Test cases for silver_listen_events asset."""

    @patch("streamify.defs.silver_assets.col")
    @patch("streamify.defs.silver_assets.row_number")
    @patch("streamify.defs.silver_assets.Window")
    def test_asset_deduplicates_rows_correctly(
        self, mock_Window, mock_row_number, mock_col
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

        # Mock PySpark functions
        mock_col.return_value = MagicMock()
        mock_col.return_value.desc.return_value = MagicMock()
        mock_row_number.return_value = MagicMock()
        mock_Window.partitionBy.return_value.orderBy.return_value = MagicMock()

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
                "spark": mock_session,
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

    @patch("streamify.defs.silver_assets.col")
    @patch("streamify.defs.silver_assets.row_number")
    @patch("streamify.defs.silver_assets.Window")
    def test_asset_returns_materialize_result(
        self, mock_Window, mock_row_number, mock_col
    ):
        """Test that asset returns MaterializeResult with expected metadata."""
        # Setup mock Spark session
        mock_session = MagicMock()

        # Create mock DataFrame chain
        mock_df = MagicMock()
        mock_with_row_num = MagicMock()
        mock_after_filter = MagicMock()
        mock_final_df = MagicMock()

        # Setup counts
        mock_df.count.return_value = 1000
        mock_final_df.count.return_value = 995

        # Setup write chain
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

        # Build chain
        mock_df.withColumn.return_value = mock_with_row_num
        mock_with_row_num.filter.return_value = mock_after_filter
        mock_after_filter.drop.return_value = mock_final_df

        mock_session.table.return_value = mock_df

        # Mock PySpark functions
        mock_col.return_value = MagicMock()
        mock_row_number.return_value = MagicMock()
        mock_Window.partitionBy.return_value.orderBy.return_value = MagicMock()

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
                "spark": mock_session,
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

        # Verify table names
        assert "bronze_listen_events" in result.metadata["source_table"].text
        assert "silver_listen_events" in result.metadata["target_table"].text


class TestSilverPageViewEventsAsset:
    """Test cases for silver_page_view_events asset."""

    @patch("streamify.defs.silver_assets.col")
    @patch("streamify.defs.silver_assets.row_number")
    @patch("streamify.defs.silver_assets.Window")
    def test_asset_deduplicates_rows_correctly(
        self, mock_Window, mock_row_number, mock_col
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

        # Mock PySpark functions
        mock_col.return_value = MagicMock()
        mock_col.return_value.desc.return_value = MagicMock()
        mock_row_number.return_value = MagicMock()
        mock_Window.partitionBy.return_value.orderBy.return_value = MagicMock()

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
                "spark": mock_session,
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

    @patch("streamify.defs.silver_assets.col")
    @patch("streamify.defs.silver_assets.row_number")
    @patch("streamify.defs.silver_assets.Window")
    def test_asset_returns_materialize_result(
        self, mock_Window, mock_row_number, mock_col
    ):
        """Test that asset returns MaterializeResult with expected metadata."""
        # Setup mock Spark session
        mock_session = MagicMock()

        # Create mock DataFrame chain
        mock_df = MagicMock()
        mock_with_row_num = MagicMock()
        mock_after_filter = MagicMock()
        mock_final_df = MagicMock()

        # Setup counts
        mock_df.count.return_value = 500
        mock_final_df.count.return_value = 490

        # Setup write chain
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

        # Build chain
        mock_df.withColumn.return_value = mock_with_row_num
        mock_with_row_num.filter.return_value = mock_after_filter
        mock_after_filter.drop.return_value = mock_final_df

        mock_session.table.return_value = mock_df

        # Mock PySpark functions
        mock_col.return_value = MagicMock()
        mock_row_number.return_value = MagicMock()
        mock_Window.partitionBy.return_value.orderBy.return_value = MagicMock()

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
                "spark": mock_session,
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

        # Verify table names
        assert "bronze_page_view_events" in result.metadata["source_table"].text
        assert "silver_page_view_events" in result.metadata["target_table"].text


class TestSilverAuthEventsAsset:
    """Test cases for silver_auth_events asset."""

    @patch("streamify.defs.silver_assets.col")
    @patch("streamify.defs.silver_assets.row_number")
    @patch("streamify.defs.silver_assets.Window")
    def test_asset_deduplicates_rows_correctly(
        self, mock_Window, mock_row_number, mock_col
    ):
        """Test that asset deduplicates rows using ROW_NUMBER window function."""
        # Setup mock Spark session
        mock_session = MagicMock()

        # Create mock DataFrame that simulates input data with duplicates
        mock_df = MagicMock()
        mock_df.count.return_value = 50  # Input rows

        # Build the chain: df -> withColumn -> filter -> drop
        mock_with_row_num = MagicMock()
        mock_after_filter = MagicMock()
        mock_final_df = MagicMock()

        # Setup count on the FINAL df (after drop)
        mock_final_df.count.return_value = 48

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

        # Mock PySpark functions
        mock_col.return_value = MagicMock()
        mock_col.return_value.desc.return_value = MagicMock()
        mock_row_number.return_value = MagicMock()
        mock_Window.partitionBy.return_value.orderBy.return_value = MagicMock()

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
                "spark": mock_session,
                "streaming_config": streaming_config,
            }
        )

        # Execute asset
        result = silver_auth_events(context)

        # Verify result type
        assert isinstance(result, dg.MaterializeResult)
        assert result.metadata is not None

        # Verify metadata
        assert result.metadata["input_rows"].value == 50
        assert result.metadata["output_rows"].value == 48
        assert result.metadata["duplicate_rows_removed"].value == 2

    @patch("streamify.defs.silver_assets.col")
    @patch("streamify.defs.silver_assets.row_number")
    @patch("streamify.defs.silver_assets.Window")
    def test_asset_returns_materialize_result(
        self, mock_Window, mock_row_number, mock_col
    ):
        """Test that asset returns MaterializeResult with expected metadata."""
        # Setup mock Spark session
        mock_session = MagicMock()

        # Create mock DataFrame chain
        mock_df = MagicMock()
        mock_with_row_num = MagicMock()
        mock_after_filter = MagicMock()
        mock_final_df = MagicMock()

        # Setup counts
        mock_df.count.return_value = 100
        mock_final_df.count.return_value = 98

        # Setup write chain
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

        # Build chain
        mock_df.withColumn.return_value = mock_with_row_num
        mock_with_row_num.filter.return_value = mock_after_filter
        mock_after_filter.drop.return_value = mock_final_df

        mock_session.table.return_value = mock_df

        # Mock PySpark functions
        mock_col.return_value = MagicMock()
        mock_row_number.return_value = MagicMock()
        mock_Window.partitionBy.return_value.orderBy.return_value = MagicMock()

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
                "spark": mock_session,
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

        # Verify table names
        assert "bronze_auth_events" in result.metadata["source_table"].text
        assert "silver_auth_events" in result.metadata["target_table"].text


class TestSilverUserSessionsAsset:
    """Test cases for silver_user_sessions asset."""

    @patch("streamify.defs.silver_assets.col")
    @patch("streamify.defs.silver_assets.lit")
    @patch("streamify.defs.silver_assets.when")
    @patch("streamify.defs.silver_assets.lag")
    @patch("streamify.defs.silver_assets.Window")
    @patch("streamify.defs.silver_assets.first")
    @patch("streamify.defs.silver_assets.sha2")
    @patch("streamify.defs.silver_assets.concat_ws")
    @patch("streamify.defs.silver_assets.min")
    @patch("streamify.defs.silver_assets.max")
    @patch("streamify.defs.silver_assets.avg")
    @patch("streamify.defs.silver_assets.spark_count")
    @patch("streamify.defs.silver_assets.spark_sum")
    def test_asset_reconstructs_sessions_correctly(
        self,
        mock_spark_sum,
        mock_spark_count,
        mock_avg,
        mock_max,
        mock_min,
        mock_concat_ws,
        mock_sha2,
        mock_first,
        mock_Window,
        mock_lag,
        mock_when,
        mock_lit,
        mock_col,
    ):
        """Test that asset reconstructs user sessions from events."""
        mock_session = MagicMock()

        # Create mock DataFrames for listen and page view events
        mock_df_listen = MagicMock()
        mock_df_page_view = MagicMock()
        mock_session.table.side_effect = [mock_df_listen, mock_df_page_view]

        # Setup select and withColumn chains
        mock_df_listen.select.return_value = mock_df_listen
        mock_df_listen.withColumn.return_value = mock_df_listen
        mock_df_page_view.select.return_value = mock_df_page_view
        mock_df_page_view.withColumn.return_value = mock_df_page_view

        # Setup union
        mock_df_listen.unionByName.return_value = mock_df_listen

        # Setup window operations chain
        mock_df_listen.withColumn.return_value = mock_df_listen
        mock_df_listen.groupBy.return_value = mock_df_listen
        mock_df_listen.agg.return_value = mock_df_listen
        mock_df_listen.count.return_value = 10

        # Setup metrics collection
        mock_metrics_row = MagicMock()
        mock_metrics_row.__getitem__ = MagicMock(return_value=300.0)
        mock_df_listen.collect.return_value = [mock_metrics_row]

        # Setup write chain
        mock_writer = MagicMock()
        mock_df_listen.write = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.format.return_value = mock_writer

        # Mock PySpark functions - create a mock that supports comparison operators
        mock_col_instance = MagicMock()
        mock_col_instance.__gt__ = MagicMock(return_value=MagicMock())
        mock_col_instance.__lt__ = MagicMock(return_value=MagicMock())
        mock_col_instance.__eq__ = MagicMock(return_value=MagicMock())
        mock_col.return_value = mock_col_instance
        mock_lit.return_value = MagicMock()
        mock_when.return_value = MagicMock()
        mock_when.return_value.otherwise = MagicMock(return_value=MagicMock())
        mock_lag.return_value = MagicMock()
        mock_first.return_value = MagicMock()
        mock_sha2.return_value = MagicMock()
        mock_concat_ws.return_value = MagicMock()
        mock_min.return_value = MagicMock()
        mock_max.return_value = MagicMock()
        mock_avg.return_value = MagicMock()
        mock_spark_count.return_value = MagicMock()
        mock_spark_sum.return_value = MagicMock()

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

        context = build_op_context(
            resources={
                "spark": mock_session,
                "streaming_config": streaming_config,
            }
        )

        result = silver_user_sessions(context)

        assert isinstance(result, dg.MaterializeResult)
        assert "session_count" in result.metadata
        assert "avg_session_duration_seconds" in result.metadata
        assert "avg_tracks_per_session" in result.metadata
        assert result.metadata["session_count"].value == 10

    @patch("streamify.defs.silver_assets.col")
    @patch("streamify.defs.silver_assets.lit")
    @patch("streamify.defs.silver_assets.when")
    @patch("streamify.defs.silver_assets.lag")
    @patch("streamify.defs.silver_assets.Window")
    @patch("streamify.defs.silver_assets.first")
    @patch("streamify.defs.silver_assets.sha2")
    @patch("streamify.defs.silver_assets.concat_ws")
    @patch("streamify.defs.silver_assets.min")
    @patch("streamify.defs.silver_assets.max")
    @patch("streamify.defs.silver_assets.avg")
    @patch("streamify.defs.silver_assets.spark_count")
    @patch("streamify.defs.silver_assets.spark_sum")
    def test_asset_returns_materialize_result_with_metadata(
        self,
        mock_spark_sum,
        mock_spark_count,
        mock_avg,
        mock_max,
        mock_min,
        mock_concat_ws,
        mock_sha2,
        mock_first,
        mock_Window,
        mock_lag,
        mock_when,
        mock_lit,
        mock_col,
    ):
        """Test that asset returns MaterializeResult with correct metadata structure."""
        mock_session = MagicMock()

        mock_df_listen = MagicMock()
        mock_df_page_view = MagicMock()
        mock_session.table.side_effect = [mock_df_listen, mock_df_page_view]

        mock_df_listen.select.return_value = mock_df_listen
        mock_df_listen.withColumn.return_value = mock_df_listen
        mock_df_page_view.select.return_value = mock_df_page_view
        mock_df_page_view.withColumn.return_value = mock_df_page_view
        mock_df_listen.unionByName.return_value = mock_df_listen
        mock_df_listen.withColumn.return_value = mock_df_listen
        mock_df_listen.groupBy.return_value = mock_df_listen
        mock_df_listen.agg.return_value = mock_df_listen
        mock_df_listen.count.return_value = 5

        mock_metrics_row = MagicMock()
        mock_metrics_row.__getitem__ = MagicMock(return_value=100.0)
        mock_df_listen.collect.return_value = [mock_metrics_row]

        mock_writer = MagicMock()
        mock_df_listen.write = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.format.return_value = mock_writer

        # Mock PySpark functions - create a mock that supports comparison operators
        mock_col_instance = MagicMock()
        mock_col_instance.__gt__ = MagicMock(return_value=MagicMock())
        mock_col_instance.__lt__ = MagicMock(return_value=MagicMock())
        mock_col_instance.__eq__ = MagicMock(return_value=MagicMock())
        mock_col.return_value = mock_col_instance
        mock_lit.return_value = MagicMock()
        mock_when.return_value = MagicMock()
        mock_when.return_value.otherwise = MagicMock(return_value=MagicMock())
        mock_lag.return_value = MagicMock()
        mock_first.return_value = MagicMock()
        mock_sha2.return_value = MagicMock()
        mock_concat_ws.return_value = MagicMock()
        mock_min.return_value = MagicMock()
        mock_max.return_value = MagicMock()
        mock_avg.return_value = MagicMock()
        mock_spark_count.return_value = MagicMock()
        mock_spark_sum.return_value = MagicMock()

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

        context = build_op_context(
            resources={
                "spark": mock_session,
                "streaming_config": streaming_config,
            }
        )

        result = silver_user_sessions(context)

        assert "session_count" in result.metadata
        assert "avg_session_duration_seconds" in result.metadata
        assert "avg_tracks_per_session" in result.metadata
        assert "source_tables" in result.metadata
        assert "target_table" in result.metadata
        assert (
            "silver_listen_events"
            in result.metadata["source_tables"].data["listen_events"]
        )
        assert (
            "silver_page_view_events"
            in result.metadata["source_tables"].data["page_view_events"]
        )
        assert "silver_user_sessions" in result.metadata["target_table"].text
