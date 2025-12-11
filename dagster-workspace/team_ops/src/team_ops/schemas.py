from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
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

SCHEMAS = {
    "listen_events": LISTEN_EVENTS_SCHEMA,
    "page_view_events": PAGE_VIEW_EVENTS_SCHEMA,
    "auth_events": AUTH_EVENTS_SCHEMA,
}
