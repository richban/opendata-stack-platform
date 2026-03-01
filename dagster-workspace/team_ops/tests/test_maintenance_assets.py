"""Tests for Dagster maintenance assets."""

from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
from dagster import build_op_context

from team_ops.defs.maintenance_assets import bronze_compaction
from team_ops.defs.resources import SparkConnectResource, StreamingJobConfig


class TestBronzeCompactionAsset:
    """Test cases for bronze_compaction asset."""

    @patch.object(SparkConnectResource, "get_session")
    def test_asset_calls_spark_sql_three_times(self, mock_get_session):
        """Test that asset calls spark.sql() 3 times (once per topic)."""
        # Setup mock Spark session
        mock_session = MagicMock()

        # Mock the SQL result with compaction stats
        mock_result = MagicMock()
        mock_result.count.return_value = 1
        mock_result.collect.return_value = [
            MagicMock(
                rewritten_data_files_count=10,
                written_data_files_size_in_bytes=1048576,
            )
        ]
        mock_session.sql.return_value = mock_result
        mock_get_session.return_value = mock_session

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

        # Build context with real resources
        context = build_op_context(
            resources={
                "spark": spark_resource,
                "streaming_config": streaming_config,
            }
        )

        # Execute asset
        result = bronze_compaction(context)

        # Verify spark.sql was called 3 times (once per topic)
        assert mock_session.sql.call_count == 3

        # Verify the calls were for correct tables
        calls = mock_session.sql.call_args_list
        expected_tables = [
            "bronze_listen_events",
            "bronze_page_view_events",
            "bronze_auth_events",
        ]

        for i, call in enumerate(calls):
            sql_query = call[0][0]
            assert "rewrite_data_files" in sql_query
            assert expected_tables[i] in sql_query

    @patch.object(SparkConnectResource, "get_session")
    def test_asset_returns_materialize_result(self, mock_get_session):
        """Test that asset returns MaterializeResult with expected metadata."""
        # Setup mock Spark session
        mock_session = MagicMock()

        # Mock the SQL result with compaction stats
        mock_result = MagicMock()
        mock_result.count.return_value = 1
        mock_result.collect.return_value = [
            MagicMock(
                rewritten_data_files_count=5,
                written_data_files_size_in_bytes=524288,
            )
        ]
        mock_session.sql.return_value = mock_result
        mock_get_session.return_value = mock_session

        # Create real resources
        spark_resource = SparkConnectResource(
            spark_remote="sc://localhost:15002",
            polaris_client_id="test-client-id",
            polaris_client_secret="test-client-secret",
            polaris_uri="http://localhost:8181",
            catalog="lakehouse",
        )

        streaming_config = StreamingJobConfig(
            kafka_bootstrap_servers="kafka:9092",
            checkpoint_path="s3a://checkpoints/streaming",
            polaris_uri="http://polaris:8181",
            polaris_client_id="test-client-id",
            polaris_client_secret="test-client-secret",
            catalog="lakehouse",
            namespace="streamify",
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
        result = bronze_compaction(context)

        # Verify result type
        assert isinstance(result, dg.MaterializeResult)
        assert result.metadata is not None

        # Verify metadata structure
        assert "tables_compacted" in result.metadata
        assert "catalog" in result.metadata
        assert "namespace" in result.metadata

        # Verify catalog and namespace values
        assert result.metadata["catalog"].text == "lakehouse"
        assert result.metadata["namespace"].text == "streamify"

    @patch.object(SparkConnectResource, "get_session")
    def test_asset_handles_empty_result(self, mock_get_session):
        """Test that asset handles empty result from rewrite_data_files."""
        # Setup mock Spark session with empty result
        mock_session = MagicMock()

        # Mock the SQL result with zero count (no files to rewrite)
        mock_result = MagicMock()
        mock_result.count.return_value = 0
        mock_session.sql.return_value = mock_result
        mock_get_session.return_value = mock_session

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

        # Execute asset - should not raise exception
        result = bronze_compaction(context)

        # Verify result
        assert isinstance(result, dg.MaterializeResult)

    @patch.object(SparkConnectResource, "get_session")
    def test_asset_handles_compaction_error(self, mock_get_session):
        """Test that asset handles errors gracefully for individual tables."""
        # Setup mock Spark session that raises on second call
        mock_session = MagicMock()

        # First and third calls succeed, second fails
        def mock_sql_side_effect(query):
            mock_result = MagicMock()

            if "page_view_events" in query:
                # Simulate error for page_view_events
                raise Exception("Table not found: bronze_page_view_events")

            mock_result.count.return_value = 1
            mock_result.collect.return_value = [
                MagicMock(
                    rewritten_data_files_count=3,
                    written_data_files_size_in_bytes=262144,
                )
            ]
            return mock_result

        mock_session.sql.side_effect = mock_sql_side_effect
        mock_get_session.return_value = mock_session

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

        # Execute asset - should not raise exception
        result = bronze_compaction(context)

        # Verify result still returned even with error
        assert isinstance(result, dg.MaterializeResult)

        # Verify error was logged in metadata
        tables_data = result.metadata["tables_compacted"].data
        assert "error" in tables_data["page_view_events"]
