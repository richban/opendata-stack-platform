from pyspark.sql.types import (
    BinaryType,
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

RAW_LISTEN_EVENTS_SCHEMA = StructType(
    LISTEN_EVENTS_SCHEMA.fields + [StructField("_corrupt_record", StringType(), True)]
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

RAW_PAGE_VIEW_EVENTS_SCHEMA = StructType(
    PAGE_VIEW_EVENTS_SCHEMA.fields + [StructField("_corrupt_record", StringType(), True)]
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

RAW_AUTH_EVENTS_SCHEMA = StructType(
    AUTH_EVENTS_SCHEMA.fields + [StructField("_corrupt_record", StringType(), True)]
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

SILVER_LISTEN_EVENTS_SCHEMA = StructType(LISTEN_EVENTS_SCHEMA.fields + META_SCHEMA)

SILVER_PAGE_VIEW_EVENTS_SCHEMA = StructType(PAGE_VIEW_EVENTS_SCHEMA.fields + META_SCHEMA)

SILVER_AUTH_EVENTS_SCHEMA = StructType(AUTH_EVENTS_SCHEMA.fields + META_SCHEMA)

SILVER_SCHEMAS = {
    "listen_events": SILVER_LISTEN_EVENTS_SCHEMA,
    "page_view_events": SILVER_PAGE_VIEW_EVENTS_SCHEMA,
    "auth_events": SILVER_AUTH_EVENTS_SCHEMA,
}

SCHEMAS = {
    "listen_events": LISTEN_EVENTS_SCHEMA,
    "page_view_events": PAGE_VIEW_EVENTS_SCHEMA,
    "auth_events": AUTH_EVENTS_SCHEMA,
}

RAW_SCHEMAS = {
    "listen_events": RAW_LISTEN_EVENTS_SCHEMA,
    "page_view_events": RAW_PAGE_VIEW_EVENTS_SCHEMA,
    "auth_events": RAW_AUTH_EVENTS_SCHEMA,
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
        StructField("_processing_date", DateType(), True),
    ]
)


# Bronze (raw, schema-on-read) + it stores the
# untouched Kafka bytes plus the resolved wire metadata.

BRONZE_SCHEMA = StructType(
    [
        StructField("raw_value", BinaryType(), True),
        StructField("wire_format", StringType(), True),
        StructField("schema_id", IntegerType(), True),
        StructField("_kafka_partition", IntegerType(), True),
        StructField("_kafka_offset", LongType(), True),
        StructField("_kafka_timestamp", TimestampType(), True),
        StructField("_ingest_time", TimestampType(), True),
        StructField("_ingest_date", DateType(), True),
    ]
)

# Records the consumer cannot yet understand (unregistered or newer schema id,
# or JSON keys outside the contract). Retryable: replay once the consumer's
# pinned contract is updated.
QUARANTINE_SCHEMA = StructType(
    [
        StructField("raw_value", BinaryType(), True),
        StructField("observed_schema_id", IntegerType(), True),
        StructField("observed_fingerprint", StringType(), True),
        StructField("expected_fingerprint", StringType(), True),
        StructField("error_stage", StringType(), True),
        StructField("error_reason", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("_kafka_partition", IntegerType(), True),
        StructField("_kafka_offset", LongType(), True),
        StructField("_kafka_timestamp", TimestampType(), True),
        StructField("_processing_time", TimestampType(), True),
        StructField("_processing_date", DateType(), True),
    ]
)
