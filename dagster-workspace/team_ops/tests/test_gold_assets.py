"""Tests for Dagster Gold layer assets."""

from unittest.mock import MagicMock, patch

import dagster as dg
from dagster import build_op_context

from team_ops.defs.gold_assets import gold_top_artists, gold_top_tracks
from team_ops.defs.resources import StreamingJobConfig


class TestGoldTopTracksAsset:
    """Test cases for gold_top_tracks asset."""

    @patch("team_ops.defs.gold_assets.col")
    @patch("team_ops.defs.gold_assets.count")
    @patch("team_ops.defs.gold_assets.countDistinct")
    @patch("team_ops.defs.gold_assets.avg")
    def test_asset_aggregates_tracks_correctly(
        self, mock_avg, mock_countDistinct, mock_count, mock_col
    ):
        """Test that asset aggregates tracks correctly with groupBy."""
        # Setup mock Spark session
        mock_session = MagicMock()

        # Create mock DataFrame that simulates input data
        mock_df = MagicMock()

        # Setup aggregation chain
        mock_grouped_df = MagicMock()
        mock_agg_df = MagicMock()
        mock_selected_df = MagicMock()

        # Setup count on the aggregated df
        mock_selected_df.count.return_value = 50

        # Setup write chain
        mock_writer = MagicMock()
        mock_selected_df.write = mock_writer
        mock_mode_writer = MagicMock()
        mock_writer.mode.return_value = mock_mode_writer
        mock_option_writer = MagicMock()
        mock_mode_writer.option.return_value = mock_option_writer
        mock_partition_writer = MagicMock()
        mock_option_writer.partitionBy.return_value = mock_partition_writer
        mock_format_writer = MagicMock()
        mock_partition_writer.format.return_value = mock_format_writer
        mock_format_writer.saveAsTable.return_value = None

        # Build chain: df.groupBy(...).agg(...).select(...)
        mock_df.groupBy.return_value = mock_grouped_df
        mock_grouped_df.agg.return_value = mock_agg_df
        mock_agg_df.select.return_value = mock_selected_df

        mock_session.table.return_value = mock_df

        # Mock PySpark functions
        mock_col.return_value = MagicMock()
        mock_count.return_value = MagicMock()
        mock_countDistinct.return_value = MagicMock()
        mock_avg.return_value = MagicMock()

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
        result = gold_top_tracks(context)

        # Verify result type
        assert isinstance(result, dg.MaterializeResult)
        assert result.metadata is not None

        # Verify metadata
        assert result.metadata["output_rows"].value == 50
        assert result.metadata["event_date"].text == "all_dates"
        assert "gold_top_tracks" in result.metadata["target_table"].text
        assert "silver_listen_events" in result.metadata["source_table"].text

        # Verify groupBy was called with correct columns
        mock_df.groupBy.assert_called_once_with("event_date", "song", "artist")


class TestGoldTopArtistsAsset:
    """Test cases for gold_top_artists asset."""

    @patch("team_ops.defs.gold_assets.col")
    @patch("team_ops.defs.gold_assets.count")
    @patch("team_ops.defs.gold_assets.countDistinct")
    def test_asset_aggregates_artists_correctly(
        self, mock_countDistinct, mock_count, mock_col
    ):
        """Test that asset aggregates artists correctly with groupBy."""
        # Setup mock Spark session
        mock_session = MagicMock()

        # Create mock DataFrame that simulates input data
        mock_df = MagicMock()

        # Setup aggregation chain
        mock_grouped_df = MagicMock()
        mock_agg_df = MagicMock()
        mock_selected_df = MagicMock()

        # Setup count on the aggregated df
        mock_selected_df.count.return_value = 25

        # Setup write chain
        mock_writer = MagicMock()
        mock_selected_df.write = mock_writer
        mock_mode_writer = MagicMock()
        mock_writer.mode.return_value = mock_mode_writer
        mock_option_writer = MagicMock()
        mock_mode_writer.option.return_value = mock_option_writer
        mock_partition_writer = MagicMock()
        mock_option_writer.partitionBy.return_value = mock_partition_writer
        mock_format_writer = MagicMock()
        mock_partition_writer.format.return_value = mock_format_writer
        mock_format_writer.saveAsTable.return_value = None

        # Build chain: df.groupBy(...).agg(...).select(...)
        mock_df.groupBy.return_value = mock_grouped_df
        mock_grouped_df.agg.return_value = mock_agg_df
        mock_agg_df.select.return_value = mock_selected_df

        mock_session.table.return_value = mock_df

        # Mock PySpark functions
        mock_col.return_value = MagicMock()
        mock_count.return_value = MagicMock()
        mock_countDistinct.return_value = MagicMock()

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
        result = gold_top_artists(context)

        # Verify result type
        assert isinstance(result, dg.MaterializeResult)
        assert result.metadata is not None

        # Verify metadata
        assert result.metadata["output_rows"].value == 25
        assert result.metadata["event_date"].text == "all_dates"
        assert "gold_top_artists" in result.metadata["target_table"].text
        assert "silver_listen_events" in result.metadata["source_table"].text

        # Verify groupBy was called with correct columns
        mock_df.groupBy.assert_called_once_with("event_date", "artist")
