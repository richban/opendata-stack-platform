"""Tests for Dagster Gold layer assets."""

from unittest.mock import MagicMock, patch

import dagster as dg
from dagster import build_op_context

from team_ops.defs.gold_assets import (
    gold_dau_mau,
    gold_top_artists,
    gold_top_tracks,
    gold_user_conversion_funnel,
)
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


class TestGoldDauMauAsset:
    """Test cases for gold_dau_mau asset."""

    @patch("team_ops.defs.gold_assets.col")
    @patch("team_ops.defs.gold_assets.countDistinct")
    @patch("team_ops.defs.gold_assets.date_trunc")
    def test_asset_computes_dau_mau_correctly(
        self, mock_date_trunc, mock_countDistinct, mock_col
    ):
        """Test that asset computes DAU and MAU correctly with groupBy and joins."""
        # Setup mock Spark session
        mock_session = MagicMock()

        # Create mock DataFrame that simulates input data
        mock_df = MagicMock()

        # Setup DAU aggregation chain (groupBy event_date)
        mock_dau_grouped_df = MagicMock()
        mock_dau_agg_df = MagicMock()
        mock_dau_selected_df = MagicMock()

        # Setup MAU aggregation chain (withColumn year_month, groupBy)
        mock_with_month_df = MagicMock()
        mock_mau_grouped_df = MagicMock()
        mock_mau_agg_df = MagicMock()
        mock_mau_selected_df = MagicMock()

        # Setup join chain
        mock_dau_with_month_df = MagicMock()
        mock_joined_df = MagicMock()
        mock_result_df = MagicMock()

        # Setup counts
        mock_result_df.count.return_value = 30  # 30 days of data
        mock_dau_selected_df.count.return_value = 30
        mock_mau_selected_df.count.return_value = 30

        # Setup first() calls for metadata
        # For dau_df.select("dau").first()[0]
        mock_dau_select = MagicMock()
        mock_dau_first = MagicMock()
        mock_dau_first.__getitem__ = MagicMock(return_value=500)  # DAU value
        mock_dau_select.first.return_value = mock_dau_first
        mock_dau_selected_df.select.return_value = mock_dau_select

        # For mau_df.select("mau").first()[0]
        mock_mau_select = MagicMock()
        mock_mau_first = MagicMock()
        mock_mau_first.__getitem__ = MagicMock(return_value=1500)  # MAU value
        mock_mau_select.first.return_value = mock_mau_first
        mock_mau_selected_df.select.return_value = mock_mau_select

        # For result_df.select("year_month").first()[0]
        mock_month_select = MagicMock()
        mock_month_row = MagicMock()
        mock_month_row.__getitem__ = MagicMock(return_value="2024-01-01 00:00:00")
        mock_month_select.first.return_value = mock_month_row
        mock_result_df.select.return_value = mock_month_select

        # Setup write chain
        mock_writer = MagicMock()
        mock_result_df.write = mock_writer
        mock_mode_writer = MagicMock()
        mock_writer.mode.return_value = mock_mode_writer
        mock_option_writer = MagicMock()
        mock_mode_writer.option.return_value = mock_option_writer
        mock_partition_writer = MagicMock()
        mock_option_writer.partitionBy.return_value = mock_partition_writer
        mock_format_writer = MagicMock()
        mock_partition_writer.format.return_value = mock_format_writer
        mock_format_writer.saveAsTable.return_value = None

        # Build DAU chain: df.groupBy("event_date").agg(...).select(...)
        mock_df.groupBy.return_value = mock_dau_grouped_df
        mock_dau_grouped_df.agg.return_value = mock_dau_agg_df
        mock_dau_agg_df.select.return_value = mock_dau_selected_df

        # Build MAU chain: df.withColumn("year_month", ...).groupBy("year_month").agg(...).select(...)
        mock_df.withColumn.return_value = mock_with_month_df
        mock_with_month_df.groupBy.return_value = mock_mau_grouped_df
        mock_mau_grouped_df.agg.return_value = mock_mau_agg_df
        mock_mau_agg_df.select.return_value = mock_mau_selected_df

        # Build join chain: dau_df.withColumn(...).join(...).select(...)
        mock_dau_selected_df.withColumn.return_value = mock_dau_with_month_df
        mock_dau_with_month_df.join.return_value = mock_joined_df
        mock_joined_df.select.return_value = mock_result_df

        mock_session.table.return_value = mock_df

        # Mock PySpark functions
        mock_col.return_value = MagicMock()
        mock_countDistinct.return_value = MagicMock()
        mock_date_trunc.return_value = MagicMock()

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
        result = gold_dau_mau(context)

        # Verify result type
        assert isinstance(result, dg.MaterializeResult)
        assert result.metadata is not None

        # Verify metadata
        assert result.metadata["output_rows"].value == 30
        assert result.metadata["event_date"].text == "all_dates"
        assert result.metadata["mau_month"].text == "2024-01-01 00:00:00"
        assert "gold_dau_mau" in result.metadata["target_table"].text
        assert "silver_listen_events" in result.metadata["source_table"].text

        # Verify groupBy was called for both DAU and MAU
        assert mock_df.groupBy.call_count >= 1
        assert mock_with_month_df.groupBy.call_count >= 1


class TestGoldUserConversionFunnelAsset:
    """Test cases for gold_user_conversion_funnel asset."""

    @patch("team_ops.defs.gold_assets.col")
    @patch("team_ops.defs.gold_assets.countDistinct")
    @patch("team_ops.defs.gold_assets.when")
    def test_asset_computes_conversion_funnel_correctly(
        self, mock_when, mock_countDistinct, mock_col
    ):
        """Test that asset computes conversion funnel correctly with groupBy and join."""
        # Setup mock Spark session
        mock_session = MagicMock()

        # Create mock DataFrame that simulates input auth events data
        mock_df_auth = MagicMock()

        # Setup free users aggregation chain
        mock_free_filtered_df = MagicMock()
        mock_free_grouped_df = MagicMock()
        mock_free_agg_df = MagicMock()
        mock_free_selected_df = MagicMock()

        # Setup paid users aggregation chain
        mock_paid_filtered_df = MagicMock()
        mock_paid_grouped_df = MagicMock()
        mock_paid_agg_df = MagicMock()
        mock_paid_selected_df = MagicMock()

        # Setup join and final calculation chain
        mock_joined_df = MagicMock()
        mock_filled_df = MagicMock()
        mock_result_df = MagicMock()

        # Setup counts - this needs to be an int, not a MagicMock
        mock_result_df.count.return_value = 30  # 30 days of data

        # Setup first() call for metadata
        mock_metrics_select = MagicMock()
        mock_metrics_row = MagicMock()
        mock_metrics_row.__getitem__ = MagicMock(
            side_effect=lambda key: {
                "free_users": 800,
                "paid_users": 200,
                "conversion_rate": 25.0,
            }.get(key, 0)
        )
        mock_metrics_select.first.return_value = mock_metrics_row
        mock_result_df.select.return_value = mock_metrics_select

        # Setup write chain
        mock_writer = MagicMock()
        mock_result_df.write = mock_writer
        mock_mode_writer = MagicMock()
        mock_writer.mode.return_value = mock_mode_writer
        mock_option_writer = MagicMock()
        mock_mode_writer.option.return_value = mock_option_writer
        mock_partition_writer = MagicMock()
        mock_option_writer.partitionBy.return_value = mock_partition_writer
        mock_format_writer = MagicMock()
        mock_partition_writer.format.return_value = mock_format_writer
        mock_format_writer.saveAsTable.return_value = None

        # Build free users chain: filter(level='free').groupBy().agg().select()
        mock_df_auth.filter.return_value = mock_free_filtered_df
        mock_free_filtered_df.groupBy.return_value = mock_free_grouped_df
        mock_free_grouped_df.agg.return_value = mock_free_agg_df
        mock_free_agg_df.select.return_value = mock_free_selected_df

        # Build paid users chain: filter(level='paid').groupBy().agg().select()
        # Need to set up a separate chain for paid users - mock filter to return different df on second call
        mock_paid_filtered_df.groupBy.return_value = mock_paid_grouped_df
        mock_paid_grouped_df.agg.return_value = mock_paid_agg_df
        mock_paid_agg_df.select.return_value = mock_paid_selected_df

        # Make filter return different values for different level filters
        def filter_side_effect(condition):
            # Check if condition is the free filter
            if isinstance(condition, MagicMock):
                # First call returns free_filtered, second returns paid_filtered
                if not hasattr(filter_side_effect, "call_count"):
                    filter_side_effect.call_count = 0
                filter_side_effect.call_count += 1
                if filter_side_effect.call_count == 1:
                    return mock_free_filtered_df
                else:
                    return mock_paid_filtered_df
            return mock_free_filtered_df

        mock_df_auth.filter.side_effect = filter_side_effect

        # Build join chain: free_df.join(paid_df, ...).select().fillna().withColumn()
        mock_free_selected_df.join.return_value = mock_joined_df
        mock_joined_df.select.return_value = mock_joined_df
        mock_joined_df.fillna.return_value = mock_filled_df
        mock_filled_df.withColumn.return_value = mock_result_df

        mock_session.table.return_value = mock_df_auth

        # Mock PySpark functions
        # Create mock column that supports comparison operators
        mock_col_instance = MagicMock()
        mock_col_instance.__gt__ = MagicMock(return_value=MagicMock())
        mock_col_instance.__lt__ = MagicMock(return_value=MagicMock())
        mock_col_instance.__eq__ = MagicMock(return_value=MagicMock())
        mock_col_instance.__truediv__ = MagicMock(return_value=MagicMock())
        mock_col_instance.__mul__ = MagicMock(return_value=MagicMock())
        mock_col.return_value = mock_col_instance

        mock_countDistinct.return_value = MagicMock()

        # Mock when() to return an object that supports .otherwise()
        mock_when_instance = MagicMock()
        mock_when_instance.otherwise.return_value = MagicMock()
        mock_when.return_value = mock_when_instance

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
        result = gold_user_conversion_funnel(context)

        # Verify result type
        assert isinstance(result, dg.MaterializeResult)
        assert result.metadata is not None

        # Verify metadata
        assert result.metadata["output_rows"].value == 30
        assert result.metadata["event_date"].text == "all_dates"
        assert result.metadata["free_users"].value == 800
        assert result.metadata["paid_users"].value == 200
        assert result.metadata["conversion_rate"].value == 25.0
        assert "gold_user_conversion_funnel" in result.metadata["target_table"].text

        # Verify filter was called for both free and paid
        assert mock_df_auth.filter.call_count >= 1


class TestGoldUserChurnAsset:
    """Test cases for gold_user_churn asset."""

    @patch("team_ops.defs.gold_assets.col")
    @patch("team_ops.defs.gold_assets.countDistinct")
    @patch("team_ops.defs.gold_assets.when")
    def test_asset_computes_churn_correctly(
        self, mock_when, mock_countDistinct, mock_col
    ):
        """Test that asset computes churn metrics correctly with groupBy and join."""
        # Setup mock Spark session
        mock_session = MagicMock()

        # Create mock DataFrame that simulates input auth events data
        mock_df_auth = MagicMock()

        # Setup churned users aggregation chain (success='false' AND level='cancelled')
        mock_churned_filtered_df = MagicMock()
        mock_churned_grouped_df = MagicMock()
        mock_churned_agg_df = MagicMock()
        mock_churned_selected_df = MagicMock()

        # Setup total users aggregation chain
        mock_total_grouped_df = MagicMock()
        mock_total_agg_df = MagicMock()
        mock_total_selected_df = MagicMock()

        # Setup join and final calculation chain
        mock_joined_df = MagicMock()
        mock_filled_df = MagicMock()
        mock_result_df = MagicMock()

        # Setup counts
        mock_result_df.count.return_value = 30  # 30 days of data

        # Setup first() call for metadata
        mock_metrics_select = MagicMock()
        mock_metrics_row = MagicMock()
        mock_metrics_row.__getitem__ = MagicMock(
            side_effect=lambda key: {
                "churned_user_count": 50,
                "churn_rate_pct": 5.0,
            }.get(key, 0)
        )
        mock_metrics_select.first.return_value = mock_metrics_row
        mock_result_df.select.return_value = mock_metrics_select

        # Setup write chain
        mock_writer = MagicMock()
        mock_result_df.write = mock_writer
        mock_mode_writer = MagicMock()
        mock_writer.mode.return_value = mock_mode_writer
        mock_option_writer = MagicMock()
        mock_mode_writer.option.return_value = mock_option_writer
        mock_partition_writer = MagicMock()
        mock_option_writer.partitionBy.return_value = mock_partition_writer
        mock_format_writer = MagicMock()
        mock_partition_writer.format.return_value = mock_format_writer
        mock_format_writer.saveAsTable.return_value = None

        # Build churned users chain: filter(...).groupBy().agg().select()
        mock_df_auth.filter.return_value = mock_churned_filtered_df
        mock_churned_filtered_df.groupBy.return_value = mock_churned_grouped_df
        mock_churned_grouped_df.agg.return_value = mock_churned_agg_df
        mock_churned_agg_df.select.return_value = mock_churned_selected_df

        # Build total users chain: groupBy().agg().select()
        mock_df_auth.groupBy.return_value = mock_total_grouped_df
        mock_total_grouped_df.agg.return_value = mock_total_agg_df
        mock_total_agg_df.select.return_value = mock_total_selected_df

        # Build join chain: churned_df.join(total_df, ...).select().fillna().withColumn()
        mock_churned_selected_df.join.return_value = mock_joined_df
        mock_joined_df.select.return_value = mock_joined_df
        mock_joined_df.fillna.return_value = mock_filled_df
        mock_filled_df.withColumn.return_value = mock_result_df

        mock_session.table.return_value = mock_df_auth

        # Mock PySpark functions
        mock_col_instance = MagicMock()
        mock_col_instance.__gt__ = MagicMock(return_value=MagicMock())
        mock_col_instance.__lt__ = MagicMock(return_value=MagicMock())
        mock_col_instance.__eq__ = MagicMock(return_value=MagicMock())
        mock_col_instance.__truediv__ = MagicMock(return_value=MagicMock())
        mock_col_instance.__mul__ = MagicMock(return_value=MagicMock())
        mock_col_instance.__and__ = MagicMock(return_value=MagicMock())
        mock_col.return_value = mock_col_instance

        mock_countDistinct.return_value = MagicMock()

        # Mock when() to return an object that supports .otherwise()
        mock_when_instance = MagicMock()
        mock_when_instance.otherwise.return_value = MagicMock()
        mock_when.return_value = mock_when_instance

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

        from team_ops.defs.gold_assets import gold_user_churn

        # Build context
        context = build_op_context(
            resources={
                "spark": mock_session,
                "streaming_config": streaming_config,
            }
        )

        # Execute asset
        result = gold_user_churn(context)

        # Verify result type
        assert isinstance(result, dg.MaterializeResult)
        assert result.metadata is not None

        # Verify metadata
        assert result.metadata["output_rows"].value == 30
        assert result.metadata["event_date"].text == "all_dates"
        assert result.metadata["churned_user_count"].value == 50
        assert result.metadata["churn_rate_pct"].value == 5.0
        assert "gold_user_churn" in result.metadata["target_table"].text

        # Verify filter was called for churn condition
        mock_df_auth.filter.assert_called_once()
