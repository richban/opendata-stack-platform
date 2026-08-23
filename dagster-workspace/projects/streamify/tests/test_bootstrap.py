"""Unit tests for pure Spark / Iceberg table management (streamify.bootstrap)."""

from unittest.mock import MagicMock
from pyspark.sql.types import StructType

from streamify.bootstrap import (
    create_namespace_if_not_exists,
    create_table_if_not_exists,
)


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

    assert mock_spark.catalog.tableExists.called
    assert mock_spark.catalog.tableExists.call_args[0][0] == "bronze_listen_events"
    assert mock_spark.sql.called
    sql_arg = mock_spark.sql.call_args[0][0]
    assert (
        "CREATE TABLE IF NOT EXISTS bronze_listen_events (user_id INT, action STRING, event_date STRING)"
        in sql_arg
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

    assert mock_spark.catalog.tableExists.called
    assert (
        mock_spark.catalog.tableExists.call_args[0][0]
        == "lakehouse.bronze.bronze_page_views"
    )
    sql_arg = mock_spark.sql.call_args[0][0]
    assert (
        "CREATE TABLE IF NOT EXISTS lakehouse.bronze.bronze_page_views (item STRING)"
        in sql_arg
    )


def test_create_table_skips_when_table_exists():
    """Verify SQL CREATE is skipped if table already exists in catalog."""
    mock_spark = MagicMock()
    mock_spark.catalog.tableExists.return_value = True

    mock_schema = MagicMock(spec=StructType)

    create_table_if_not_exists(
        mock_spark,
        table_name="bronze_page_views",
        schema=mock_schema,
    )

    assert mock_spark.catalog.tableExists.called
    assert not mock_spark.sql.called
