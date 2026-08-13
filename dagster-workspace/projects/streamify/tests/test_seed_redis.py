import asyncio
import datetime

from unittest.mock import AsyncMock, MagicMock

import pytest

from pydantic import ValidationError
from streamify.seed_redis import UserProfile, flush_batch_to_redis, should_flush
from streamify import seed_redis

USER_ID = 42
EVENT_TS = datetime.datetime(2025, 12, 31, 12, 0, 0, tzinfo=datetime.timezone.utc)


@pytest.fixture
def mock_avro_input():
    return {
        "USERID": USER_ID,
        "FIRSTNAME": "Jane",
        "LASTNAME": "Doe",
        "GENDER": "F",
        "CITY": "Seattle",
        "STATE": "WA",
        "ZIP": "98101",
        "EVENT_TIME": EVENT_TS,
        "INGESTION_TIME": EVENT_TS,
    }


@pytest.fixture
def profile(mock_avro_input):
    return UserProfile.model_validate(mock_avro_input)


@pytest.fixture
def redis_resources():
    """Mock redis dependencies."""
    pipe = AsyncMock()
    pipe.hset = MagicMock()
    pipe.execute = AsyncMock()
    pipe.__aenter__.return_value = pipe

    redis_client = MagicMock()
    redis_client.pipeline = MagicMock(return_value=pipe)

    consumer = MagicMock()

    return redis_client, pipe, consumer


class FakeMessage:
    """Stand-in for a confluent_kafka Message (real class, not a mock)."""

    def __init__(self, value=b"data", error=None, topic="user_profiles"):
        self._value = value
        self._error = error
        self._topic = topic

    def error(self):
        return self._error

    def topic(self):
        return self._topic

    def value(self):
        return self._value


@pytest.fixture
def main_resources(mocker):
    """Mock every external dependency of main()."""

    cfg = MagicMock()
    cfg.redis_host = "localhost"
    cfg.redis_port = 6379
    cfg.schema_registry_url = "http://localhost:8081"
    cfg.kafka_bootstrap_servers = "localhost:9093"
    mocker.patch("streamify.seed_redis.get_streaming_config", return_value=cfg)

    redis_client = AsyncMock()
    redis_client.ping = AsyncMock(return_value=True)
    redis_client.aclose = AsyncMock()
    mocker.patch("streamify.seed_redis.aioredis.Redis", return_value=redis_client)

    schema_client = MagicMock()
    mocker.patch(
        "streamify.seed_redis.AsyncSchemaRegistryClient", return_value=schema_client
    )
    deserializer = AsyncMock()  # the instance
    mocker.patch(
        "streamify.seed_redis.AsyncAvroDeserializer",
        AsyncMock(return_value=deserializer),
    )  # the Class

    consumer = MagicMock()
    consumer.subscribe = MagicMock()
    consumer.close = MagicMock()
    mocker.patch("streamify.seed_redis.Consumer", return_value=consumer)

    return cfg, redis_client, schema_client, deserializer, consumer


class TestUserProfile:
    def test_uppercase_avro_fields_map_to_snake_case(self, mock_avro_input):
        profile = UserProfile.model_validate(mock_avro_input)

        assert profile.user_id == USER_ID
        assert profile.first_name == "Jane"
        assert profile.zip_code == "98101"

    def test_timestamp_fields_are_coerced_to_datetime(self, mock_avro_input):
        profile = UserProfile.model_validate(mock_avro_input)

        assert isinstance(profile.event_time, datetime.datetime)
        assert isinstance(profile.ingestion_time, datetime.datetime)
        assert profile.event_time == EVENT_TS

    def test_iso_string_timestamps_are_parsed_to_datetime(self, mock_avro_input):
        mock_avro_input["EVENT_TIME"] = "2025-12-31T12:00:00Z"

        profile = UserProfile.model_validate(mock_avro_input)

        assert profile.event_time == EVENT_TS

    def test_missing_avro_fields_default_to_none(self):
        profile = UserProfile.model_validate({"FIRSTNAME": "Jane"})

        assert profile.first_name == "Jane"
        assert profile.user_id is None
        assert profile.zip_code is None
        assert profile.event_time is None


class TestShouldFlush:
    @pytest.mark.unit
    def test_empty_batch_neither_trigger_returns_false(self):
        assert should_flush([], 0.0) is False

    @pytest.mark.unit
    def test_size_trigger_ignores_time(self):
        batch = [None] * 1000
        assert should_flush(batch, 0.0) is True

    @pytest.mark.unit
    def test_time_trigger_with_nonempty_batch(self):
        assert should_flush([UserProfile()], 1.0) is True

    @pytest.mark.unit
    def test_time_below_threshold_returns_false(self):
        assert should_flush([UserProfile()], 0.9) is False

    @pytest.mark.unit
    def test_empty_batch_does_not_flush_on_time(self):
        assert should_flush([], 5.0) is False


class TestFlushBatchToRedis:
    @pytest.mark.anyio
    async def test_writes_hset_with_correct_key_and_mappings(
        self, redis_resources, profile
    ):
        redis_client, pipe, consumer = redis_resources

        await flush_batch_to_redis([profile], redis_client, consumer)

        pipe.hset.assert_called_once_with(
            "user:42",
            mapping={
                "first_name": "Jane",
                "last_name": "Doe",
                "gender": "F",
                "city": "Seattle",
                "state": "WA",
                "zip_code": "98101",
                "event_time": "2025-12-31T12:00:00Z",
                "ingestion_time": "2025-12-31T12:00:00Z",
            },
        )

    @pytest.mark.anyio
    async def test_skip_profile_without_user_id(self, redis_resources):
        redis_client, pipe, consumer = redis_resources
        anonymous = UserProfile.model_validate({"FIRSTNAME": "Jane"})

        await flush_batch_to_redis([anonymous], redis_client, consumer)

        pipe.hset.assert_not_called()
        consumer.commit.assert_called_once_with(asynchronous=True)

    @pytest.mark.anyio
    async def test_commits_offset_asynchronously(self, redis_resources, profile):
        redis_client, pipe, consumer = redis_resources

        await flush_batch_to_redis([profile], redis_client, consumer)

        consumer.commit.assert_called_once_with(asynchronous=True)

    @pytest.mark.anyio
    async def test_returns_float_duration(self, redis_resources, profile):
        redis_client, pipe, consumer = redis_resources

        duration = await flush_batch_to_redis([profile], redis_client, consumer)

        assert isinstance(duration, float)
        assert duration >= 0.0

    @pytest.mark.anyio
    async def test_empty_batch_still_executes_and_commits(self, redis_resources):
        redis_client, pipe, consumer = redis_resources

        await flush_batch_to_redis([], redis_client, consumer)

        pipe.hset.assert_not_called()
        pipe.execute.assert_awaited_once()
        consumer.commit.assert_called_once_with(asynchronous=True)


class TestMain:
    @staticmethod
    def _force_time_trigger(mocker):
        """Fake the clock: last_flush=0.0, now=1001.0ms, flush-internal 1.0/1.005."""
        mocker.patch(
            "streamify.seed_redis.time.perf_counter",
            side_effect=[0.0, 2.0, 1.0, 1.005],
        )

    @pytest.mark.anyio
    async def test_deserializes_message_and_flushes_on_time_trigger(
        self, main_resources, mocker, profile
    ):
        cfg, redis_client, schema_client, deserializer, consumer = main_resources

        deserializer.return_value = profile
        consumer.consume.side_effect = [[FakeMessage()], KeyboardInterrupt]

        flush_mock = AsyncMock(return_value=0.001)
        mocker.patch.object(seed_redis, "flush_batch_to_redis", flush_mock)
        self._force_time_trigger(mocker)

        await seed_redis.main()

        flush_mock.assert_awaited_once()
        consumer.close.assert_called_once()
        redis_client.aclose.assert_awaited_once()
