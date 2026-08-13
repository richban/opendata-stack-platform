import asyncio
import datetime

from unittest.mock import AsyncMock, MagicMock

import pytest

from pydantic import ValidationError
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
