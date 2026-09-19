from unittest.mock import MagicMock, patch

import pytest

from streamify.defs.resources import (
    StreamingJobConfig,
    create_spark_session,
    get_streaming_config,
)
from streamify.main import StreamifyDeclarativePipeline
from streamify.schema_registry import StreamSchemaConfig
from streamify.wire import WireFormat


class TestGetStreamingConfig:
    @pytest.fixture(autouse=True)
    def bootstrap_cache(self):
        """
        Its a module level singleton and tests share state.
        Env changes won't apply unless the caches is cleared.
        """
        get_streaming_config.cache_clear()
        yield
        get_streaming_config.cache_clear()

    def test_same_instance(self):
        config_a = get_streaming_config()
        config_b = get_streaming_config()
        assert config_a is config_b
        assert id(config_a) == id(config_b)

    def test_empty_str_spark_remote_env(self, monkeypatch):
        """
        OS env vars override dotenv files.

        pydantic-settings priority (highest → lowest):

        1. init args (StreamingJobConfig(spark_remote=...))
        2. OS environment variables ← monkeypatch.setenv patches here
        3. dotenv files (.env, .env.dev, .env.polaris)
        4. secrets
        5. defaults
        """
        monkeypatch.setenv("SPARK_REMOTE", "")
        cfg = get_streaming_config()
        assert cfg.spark_remote is None


class TestWireFormatRouting:
    def test_wire_format_lookup(self):
        pipeline = StreamifyDeclarativePipeline(
            spark=MagicMock(),
            config=StreamingJobConfig(),
            source=MagicMock(),
            songs_enricher=MagicMock(),
            redis_enricher=MagicMock(),
            bronze_sink=MagicMock(),
            clickhouse_sink=MagicMock(),
            dlq_sink=MagicMock(),
            clickhouse=MagicMock(),
            schema_config=StreamSchemaConfig(
                wire_format_by_topic={
                    "listen_events": WireFormat.JSON,
                    "user_profiles": WireFormat.AVRO,
                },
            ),
        )

        assert pipeline.wire_format_for("listen_events") is WireFormat.JSON
        assert pipeline.wire_format_for("user_profiles") is WireFormat.AVRO
        assert pipeline.wire_format_for("unknown") is WireFormat.JSON


class TestCreateSparkSession:
    @pytest.fixture
    def mock_builder(self):
        builder = MagicMock()
        session = MagicMock()
        builder.getOrCreate.return_value = session

        for name in ("appName", "remote", "master", "config"):
            getattr(builder, name).return_value = builder

        with patch("pyspark.sql.SparkSession.builder", builder):
            yield builder

    def test_configures_spark_connect(self, mock_builder):
        cfg = StreamingJobConfig(
            spark_remote="sc://localhost:15002",
            catalog="lakehouse",
            namespace="streamify",
            polaris_client_id="cid",
            polaris_client_secret="secret",
            polaris_uri="http://polaris:8181/api/catalog",
        )
        session = create_spark_session(cfg, app_name="TestApp")

        mock_builder.remote.assert_called_once_with("sc://localhost:15002")
        mock_builder.master.assert_not_called()

        session.sql.assert_any_call("CREATE NAMESPACE IF NOT EXISTS lakehouse.streamify")
        session.sql.assert_any_call("USE lakehouse.streamify")

    def test_configures_spark_local(self, mock_builder):
        cfg = StreamingJobConfig(
            spark_remote="",
            catalog="lakehouse",
            namespace="streamify",
        )
        session = create_spark_session(cfg, app_name="TestApp")

        mock_builder.master.assert_called_once_with("local[*]")
        mock_builder.remote.assert_not_called()

        session.sql.assert_not_called()
