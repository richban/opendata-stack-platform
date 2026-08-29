import pytest

from unittest.mock import MagicMock, patch
from streamify.defs.resources import (
    get_streaming_config,
    StreamingJobConfig,
    create_spark_session,
)


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
