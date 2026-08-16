import datetime

from collections.abc import Generator
from contextlib import contextmanager
from typing import NamedTuple
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from streamify import seed_redis
from streamify.seed_redis import UserProfile, flush_batch_to_redis, should_flush

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


class MainMocks(NamedTuple):
    cfg: MagicMock
    redis_client: MagicMock
    pipe: MagicMock
    schema_client: MagicMock
    deserializer: AsyncMock
    consumer: MagicMock


@pytest.fixture
def mock_redis_kafka() -> MainMocks:
    """Static mock collaborators for main() (no patching)."""
    cfg = MagicMock()
    cfg.redis_host = "localhost"
    cfg.redis_port = 6379
    cfg.schema_registry_url = "http://localhost:8081"
    cfg.kafka_bootstrap_servers = "localhost:9093"

    pipe = AsyncMock()
    pipe.hset = MagicMock()
    pipe.execute = AsyncMock()
    pipe.__aenter__.return_value = pipe

    redis_client = MagicMock()
    redis_client.pipeline = MagicMock(return_value=pipe)
    redis_client.ping = AsyncMock(return_value=True)
    redis_client.aclose = AsyncMock()

    schema_client = MagicMock()
    deserializer = AsyncMock()

    consumer = MagicMock()
    consumer.subscribe = MagicMock()
    consumer.close = MagicMock()

    return MainMocks(
        cfg=cfg,
        redis_client=redis_client,
        pipe=pipe,
        schema_client=schema_client,
        deserializer=deserializer,
        consumer=consumer,
    )


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


@contextmanager
def mock_main_dependencies(mocks: MainMocks) -> Generator[MainMocks, None, None]:
    """Wire the static mocks into seed_redis's module-level constructor calls."""
    with (
        patch.object(seed_redis, "get_streaming_config", return_value=mocks.cfg),
        patch("streamify.seed_redis.aioredis.Redis", return_value=mocks.redis_client),
        patch.object(
            seed_redis, "AsyncSchemaRegistryClient", return_value=mocks.schema_client
        ),
        patch.object(
            seed_redis,
            "AsyncAvroDeserializer",
            AsyncMock(return_value=mocks.deserializer),
        ),
        patch.object(seed_redis, "Consumer", return_value=mocks.consumer),
    ):
        yield mocks


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
    def test_empty_batch_neither_trigger_returns_false(self):
        assert should_flush([], 0.0) is False

    def test_size_trigger_ignores_time(self):
        batch = [None] * 1000
        assert should_flush(batch, 0.0) is True

    def test_time_trigger_with_nonempty_batch(self):
        assert should_flush([UserProfile()], 1.0) is True

    def test_time_below_threshold_returns_false(self):
        assert should_flush([UserProfile()], 0.9) is False

    def test_empty_batch_does_not_flush_on_time(self):
        assert should_flush([], 5.0) is False


class TestFlushBatchToRedis:
    @pytest.mark.anyio
    async def test_writes_hset_with_correct_key_and_mappings(
        self, profile, mock_redis_kafka
    ):
        await flush_batch_to_redis(
            [profile], mock_redis_kafka.redis_client, mock_redis_kafka.consumer
        )

        mock_redis_kafka.pipe.hset.assert_called_once_with(
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
    async def test_skip_profile_without_user_id(self, mock_redis_kafka):
        anonymous = UserProfile.model_validate({"FIRSTNAME": "Jane"})

        await flush_batch_to_redis(
            [anonymous], mock_redis_kafka.redis_client, mock_redis_kafka.consumer
        )

        mock_redis_kafka.pipe.hset.assert_not_called()
        mock_redis_kafka.consumer.commit.assert_called_once_with(asynchronous=True)

    @pytest.mark.anyio
    async def test_commits_offset_asynchronously(self, profile, mock_redis_kafka):
        await flush_batch_to_redis(
            [profile], mock_redis_kafka.redis_client, mock_redis_kafka.consumer
        )

        mock_redis_kafka.consumer.commit.assert_called_once_with(asynchronous=True)

    @pytest.mark.anyio
    async def test_returns_float_duration(self, profile, mock_redis_kafka):
        duration = await flush_batch_to_redis(
            [profile], mock_redis_kafka.redis_client, mock_redis_kafka.consumer
        )

        assert isinstance(duration, float)
        assert duration >= 0.0

    @pytest.mark.anyio
    async def test_empty_batch_still_executes_and_commits(self, mock_redis_kafka):
        await flush_batch_to_redis(
            [], mock_redis_kafka.redis_client, mock_redis_kafka.consumer
        )

        mock_redis_kafka.pipe.hset.assert_not_called()
        mock_redis_kafka.pipe.execute.assert_awaited_once()
        mock_redis_kafka.consumer.commit.assert_called_once_with(asynchronous=True)


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
        self, mock_redis_kafka, mocker, profile
    ):
        with mock_main_dependencies(mock_redis_kafka) as mocks:
            mocks.deserializer.return_value = profile
            mocks.consumer.consume.side_effect = [[FakeMessage()], KeyboardInterrupt]

            flush_mock = AsyncMock(return_value=0.001)
            mocker.patch.object(seed_redis, "flush_batch_to_redis", flush_mock)
            self._force_time_trigger(mocker)

            await seed_redis.main()

            flush_mock.assert_awaited_once()
            mocks.consumer.close.assert_called_once()
            mocks.redis_client.aclose.assert_awaited_once()

    @pytest.mark.anyio
    async def test_flushes_when_batch_size_is_reached(
        self, mock_redis_kafka, mocker, profile
    ):
        with mock_main_dependencies(mock_redis_kafka) as mocks:
            mocks.deserializer.return_value = profile
            mocks.consumer.consume.side_effect = [
                [FakeMessage()] * 1000,
                KeyboardInterrupt,
            ]

            flush_mock = AsyncMock(return_value=0.001)
            mocker.patch.object(seed_redis, "flush_batch_to_redis", flush_mock)
            mocker.patch(
                "streamify.seed_redis.time.perf_counter",
                side_effect=[0.0, 0.5, 1.0, 1.005],
            )

            await seed_redis.main()

            flush_mock.assert_awaited_once()

    @pytest.mark.anyio
    async def test_skips_messages_with_consumer_error(
        self, mock_redis_kafka, mocker, caplog
    ):
        with mock_main_dependencies(mock_redis_kafka) as mocks:
            bad_msg = FakeMessage(error="partition error")
            mocks.consumer.consume.side_effect = [
                [bad_msg, FakeMessage()],
                KeyboardInterrupt,
            ]

            flush_mock = AsyncMock(return_value=0.001)
            mocker.patch.object(seed_redis, "flush_batch_to_redis", flush_mock)
            mocker.patch(
                "streamify.seed_redis.time.perf_counter",
                side_effect=[0.0, 1.1, 1.0, 1.005],
            )

            await seed_redis.main()

            assert "Kafka consumer error: partition error" in caplog.text

            # topic is None on the error message -> skipped, no flush
            flush_mock.assert_awaited_once()
            mocks.deserializer.assert_awaited_once()

    @pytest.mark.anyio
    async def test_skips_messages_without_topic(self, mock_redis_kafka, mocker, caplog):
        with mock_main_dependencies(mock_redis_kafka) as mocks:
            no_topic = FakeMessage(topic=None)
            mocks.consumer.consume.side_effect = [
                [no_topic, FakeMessage()],
                KeyboardInterrupt,
            ]

            flush_mock = AsyncMock(return_value=0.001)
            mocker.patch.object(seed_redis, "flush_batch_to_redis", flush_mock)
            mocker.patch(
                "streamify.seed_redis.time.perf_counter",
                side_effect=[0.0, 2.0, 1.0, 1.005],
            )

            await seed_redis.main()

            assert "Received message without valid topic name" in caplog.text

            flush_mock.assert_awaited_once()
            mocks.consumer.close.assert_called_once()
            mocks.redis_client.aclose.assert_awaited_once()

    @pytest.mark.anyio
    async def test_cleanup_runs_on_keyboard_interrupt(self, mock_redis_kafka, mocker):
        with mock_main_dependencies(mock_redis_kafka) as mocks:
            mocks.consumer.consume.side_effect = [KeyboardInterrupt]

            await seed_redis.main()

            mocks.consumer.close.assert_called_once()
            mocks.redis_client.aclose.assert_awaited_once()

    @pytest.mark.anyio
    async def test_end_to_end_flush_writes_to_redis(
        self, mock_redis_kafka, mocker, profile
    ):
        """Real flush_batch_to_redis + mocked redis pipeline."""
        with mock_main_dependencies(mock_redis_kafka) as mocks:
            mocks.deserializer.return_value = profile
            mocks.consumer.consume.side_effect = [
                [FakeMessage()],
                KeyboardInterrupt,
            ]
            self._force_time_trigger(mocker)

            await seed_redis.main()

            mocks.pipe.hset.assert_called_once_with(
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
            mocks.pipe.execute.assert_awaited_once()
            mocks.consumer.commit.assert_called_once_with(asynchronous=True)
            mocks.consumer.close.assert_called_once()
            mocks.redis_client.aclose.assert_awaited_once()
