"""Tests for Dagster resource definitions."""

from unittest.mock import MagicMock, patch

import dagster as dg

from streamify.defs.resources import (
    StreamingJobConfig,
    create_clickhouse_resource,
    create_kafka_consumer_resource,
    create_redis_resource,
    create_s3_resource,
    create_schema_registry_resource,
    get_streaming_config,
    spark_resource,
)

EXPECTED_CH_PORT = 9000
EXPECTED_REDIS_PORT = 6380


class TestSparkResource:
    """Test cases for spark_resource definition."""

    @patch("streamify.defs.resources.create_spark_session")
    def test_spark_resource_extracts_config_from_context(self, mock_create_spark):
        """Test that spark_resource extracts streaming_config from InitResourceContext."""
        mock_session = MagicMock()
        mock_create_spark.return_value = mock_session

        custom_config = StreamingJobConfig(
            polaris_uri="http://custom-polaris:8181/api/catalog",
            catalog="custom_catalog",
            namespace="custom_namespace",
        )

        ctx = dg.build_init_resource_context(
            resources={"streaming_config": custom_config}
        )

        result = spark_resource(ctx)

        mock_create_spark.assert_called_once_with(custom_config)
        assert result == mock_session

    @patch("streamify.defs.resources.create_spark_session")
    def test_spark_resource_with_default_streaming_config(self, mock_create_spark):
        """Test that spark_resource works with default streaming_config in context."""
        mock_session = MagicMock()
        mock_create_spark.return_value = mock_session

        default_config = get_streaming_config()
        ctx = dg.build_init_resource_context(
            resources={"streaming_config": default_config}
        )

        result = spark_resource(ctx)

        mock_create_spark.assert_called_once_with(default_config)
        assert result == mock_session


class TestOtherResourceFactories:
    """Test cases for other resource factory functions."""

    def test_create_s3_resource(self):
        config = StreamingJobConfig(
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
            aws_endpoint_url="http://minio:9000",
        )
        s3 = create_s3_resource(config)
        assert s3.aws_access_key_id == "test_key"
        assert s3.aws_secret_access_key == "test_secret"
        assert s3.endpoint_url == "http://minio:9000"

    def test_create_clickhouse_resource(self):
        config = StreamingJobConfig(
            clickhouse_host="ch_host",
            clickhouse_port=EXPECTED_CH_PORT,
            clickhouse_user="ch_user",
            clickhouse_password="ch_pass",
            clickhouse_db="ch_db",
        )
        ch = create_clickhouse_resource(config)
        assert ch.host == "ch_host"
        assert ch.port == EXPECTED_CH_PORT
        assert ch.username == "ch_user"
        assert ch.password == "ch_pass"
        assert ch.database == "ch_db"

    def test_create_redis_resource(self):
        config = StreamingJobConfig(
            redis_host="redis_host",
            redis_port=EXPECTED_REDIS_PORT,
        )
        redis_res = create_redis_resource(config)
        assert redis_res.host == "redis_host"
        assert redis_res.port == EXPECTED_REDIS_PORT

    def test_create_schema_registry_resource(self):
        config = StreamingJobConfig(schema_registry_url="http://sr:8081")
        sr_res = create_schema_registry_resource(config)
        assert sr_res.url == "http://sr:8081"

    def test_create_kafka_consumer_resource(self):
        config = StreamingJobConfig(kafka_bootstrap_servers="kafka:9092")
        kafka_res = create_kafka_consumer_resource(config, group_id="test_group")
        assert kafka_res.bootstrap_servers == "kafka:9092"
        assert kafka_res.group_id == "test_group"
