from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

LISTEN_EVENTS_SCHEMA = StructType(
    [
        StructField("artist", StringType(), True),
        StructField("song", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("ts", LongType(), True),
        StructField("auth", StringType(), True),
        StructField("level", StringType(), True),
        StructField("city", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("state", StringType(), True),
        StructField("userAgent", StringType(), True),
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True),
        StructField("userId", LongType(), True),
        StructField("lastName", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("registration", LongType(), True),
        StructField("sessionId", IntegerType(), True),
        StructField("itemInSession", IntegerType(), True),
    ]
)

PAGE_VIEW_EVENTS_SCHEMA = StructType(
    [
        StructField("ts", LongType(), True),
        StructField("sessionId", IntegerType(), True),
        StructField("auth", StringType(), True),
        StructField("level", StringType(), True),
        StructField("itemInSession", IntegerType(), True),
        StructField("city", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("state", StringType(), True),
        StructField("userAgent", StringType(), True),
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True),
        StructField("userId", LongType(), True),
        StructField("lastName", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("registration", LongType(), True),
        StructField("page", StringType(), True),
    ]
)

AUTH_EVENTS_SCHEMA = StructType(
    [
        StructField("ts", LongType(), True),
        StructField("sessionId", IntegerType(), True),
        StructField("level", StringType(), True),
        StructField("itemInSession", IntegerType(), True),
        StructField("city", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("state", StringType(), True),
        StructField("userAgent", StringType(), True),
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True),
        StructField("userId", LongType(), True),
        StructField("lastName", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("registration", LongType(), True),
        StructField("success", StringType(), True),
    ]
)

META_SCHEMA = [
    StructField("event_id", StringType(), True),
    StructField("event_ts", TimestampType(), True),
    StructField("event_date", DateType(), True),
    StructField("_kafka_partition", IntegerType(), True),
    StructField("_kafka_offset", LongType(), True),
    StructField("_kafka_timestamp", TimestampType(), True),
    StructField("_processing_time", TimestampType(), True),
]

ENRICHED_USER_PROFILE_SCHEMA = StructType(
    [
        StructField("enriched_first_name", StringType(), True),
        StructField("enriched_last_name", StringType(), True),
        StructField("enriched_gender", StringType(), True),
        StructField("enriched_city", StringType(), True),
        StructField("enriched_state", StringType(), True),
        StructField("enriched_zip", StringType(), True),
    ]
)

SCHEMAS = {
    "listen_events": LISTEN_EVENTS_SCHEMA,
    "page_view_events": PAGE_VIEW_EVENTS_SCHEMA,
    "auth_events": AUTH_EVENTS_SCHEMA,
}

DLQ_SCHEMA = StructType(
    [
        StructField("raw_payload", StringType(), True),
        StructField("error_stage", StringType(), True),
        StructField("error_reason", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("_kafka_partition", IntegerType(), True),
        StructField("_kafka_offset", LongType(), True),
        StructField("_kafka_timestamp", TimestampType(), True),
        StructField("_processing_time", TimestampType(), True),
    ]
)

# ---------------------------------------------------------------------------
# ClickHouse null defaults (single source of truth)
# ---------------------------------------------------------------------------

CLICKHOUSE_NULL_DEFAULTS: dict[str, int | float | str] = {
    "event_id": "",
    "user_id": 0,
    "artist": "",
    "song": "",
    "duration": 0.0,
    "session_id": "",
    "city": "",
    "state": "",
    "enriched_first_name": "",
    "enriched_last_name": "",
    "enriched_gender": "",
    "enriched_city": "",
    "enriched_state": "",
    "enriched_zip": "",
    "song_year": "",
    "artist_location": "",
}

# Ordered list of columns written to ClickHouse
CLICKHOUSE_COLUMNS: list[str] = [
    "event_id",
    "user_id",
    "artist",
    "song",
    "duration",
    "event_ts",
    "session_id",
    "city",
    "state",
    "enriched_first_name",
    "enriched_last_name",
    "enriched_gender",
    "enriched_city",
    "enriched_state",
    "enriched_zip",
    "song_year",
    "artist_location",
    "_processing_time",
]


# Field names fetched from each ``user:<id>`` Redis hash, ordered to match
# ``ENRICHED_USER_PROFILE_SCHEMA``.
PROFILE_FIELDS: tuple[str, ...] = (
    "first_name",
    "last_name",
    "gender",
    "city",
    "state",
    "zip_code",
)
