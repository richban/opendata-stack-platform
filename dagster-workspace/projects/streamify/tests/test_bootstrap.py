"""Unit tests for pure Spark / Iceberg table management (streamify.bootstrap)."""

from unittest.mock import MagicMock, patch

from dagster_aws.s3 import S3Resource
from pyspark.sql.types import StructType
from streamify.bootstrap import (
    bootstrap_storage,
    create_namespace_if_not_exists,
    create_table_if_not_exists,
    ensure_clickhouse_table_exists,
)
from streamify.defs.resources import (
    ClickHouseResource,
    StreamingJobConfig,
    create_s3_resource,
)

EXPECTED_DDL_CALLS = 2


def test_create_namespace_if_not_exists():
    """Verify namespace creation executes CREATE NAMESPACE IF NOT EXISTS."""
    mock_spark = MagicMock()
    create_namespace_if_not_exists(mock_spark, "streamify", "bronze")

    assert mock_spark.sql.called
    sql_arg = mock_spark.sql.call_args[0][0]
    assert sql_arg == "CREATE NAMESPACE IF NOT EXISTS streamify.bronze"


def test_create_table_if_not_exists_session_agnostic():
    """Verify table creation with session-agnostic table name."""
    mock_spark = MagicMock()
    mock_spark.catalog.tableExists.return_value = False

    mock_schema = MagicMock(spec=StructType)
    mock_schema.toDDL.return_value = "user_id INT, action STRING, event_date STRING"

    create_table_if_not_exists(
        mock_spark,
        table_name="bronze_listen_events",
        schema=mock_schema,
        partition_col="event_date",
    )

    assert mock_spark.sql.called
    sql_arg = mock_spark.sql.call_args[0][0]
    assert (
        "CREATE TABLE IF NOT EXISTS bronze_listen_events "
        "(user_id INT, action STRING, event_date STRING)" in sql_arg
    )
    assert "USING iceberg PARTITIONED BY (event_date)" in sql_arg


def test_create_table_if_not_exists_fully_qualified():
    """Verify table creation with explicit catalog and namespace."""
    mock_spark = MagicMock()
    mock_spark.catalog.tableExists.return_value = False

    mock_schema = MagicMock(spec=StructType)
    mock_schema.toDDL.return_value = "item STRING"

    create_table_if_not_exists(
        mock_spark,
        table_name="bronze_page_views",
        schema=mock_schema,
        catalog="lakehouse",
        namespace="bronze",
    )

    sql_arg = mock_spark.sql.call_args[0][0]
    assert (
        "CREATE TABLE IF NOT EXISTS lakehouse.bronze.bronze_page_views (item STRING)"
        in sql_arg
    )




def test_ensure_clickhouse_table_exists():
    """Verify ClickHouse database and table creation via ClickHouseResource."""
    mock_resource = MagicMock(spec=ClickHouseResource)
    mock_resource.database = "test_db"
    mock_client = MagicMock()
    mock_resource.get_client.return_value = mock_client

    ensure_clickhouse_table_exists(mock_resource)

    assert mock_client.command.call_count == EXPECTED_DDL_CALLS
    create_db_call = mock_client.command.call_args_list[0][0][0]
    assert "CREATE DATABASE IF NOT EXISTS test_db" in create_db_call
    create_table_call = mock_client.command.call_args_list[1][0][0]
    assert "silver_playback_events" in create_table_call
    mock_client.close.assert_called_once()


def test_bootstrap_storage():
    """Verify bootstrap_storage executes both ClickHouse and Iceberg bootstrapping."""
    mock_spark = MagicMock()
    mock_resource = MagicMock(spec=ClickHouseResource)
    mock_resource.database = "streamify"
    mock_client = MagicMock()
    mock_resource.get_client.return_value = mock_client

    with patch("streamify.bootstrap.create_table_if_not_exists") as mock_create_table:
        bootstrap_storage(
            spark=mock_spark,
            clickhouse=mock_resource,
            topics=["listen_events"],
            catalog="lakehouse",
            namespace="streamify",
        )

        # ClickHouse DDL calls
        assert mock_client.command.call_count == EXPECTED_DDL_CALLS
        # Spark table creation calls (1 bronze + 1 DLQ)
        assert mock_create_table.call_count == EXPECTED_DDL_CALLS


def test_create_s3_resource():
    """Verify create_s3_resource returns an S3Resource with expected attributes."""
    cfg = StreamingJobConfig(
        aws_access_key_id="test_key",
        aws_secret_access_key="test_secret",
        aws_endpoint_url="http://localhost:9000",
    )
    s3 = create_s3_resource(cfg)
    assert isinstance(s3, S3Resource)
    assert s3.aws_access_key_id == "test_key"
    assert s3.aws_secret_access_key == "test_secret"
    assert s3.endpoint_url == "http://localhost:9000"
