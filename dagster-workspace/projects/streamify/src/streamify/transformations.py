from collections.abc import Iterable, Iterator
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructType
import logging

from pyspark.sql.functions import col

from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    current_timestamp,
    from_json,
    lit,
    sha2,
    to_date,
    udf,
)
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from streamify.schemas import (
    CLICKHOUSE_NULL_DEFAULTS,
    ENRICHED_USER_PROFILE_SCHEMA,
    PROFILE_FIELDS,
    RAW_SCHEMAS,
)

logger = logging.getLogger(__name__)

from streamify.defs.resources import (
    ClickHouseResource,
    S3Resource,
    StreamingJobConfig,
    create_clickhouse_resource,
    create_s3_resource,
    create_spark_session,
    get_executor_clickhouse_client,
    get_executor_redis_client,
    get_streaming_config,
)


@udf(returnType=StringType())
def decode_escaped_string(s: str | None) -> str | None:
    """Decode unicode/octal-escaped strings (e.g. artist/song names)."""
    if not s:
        return s
    try:
        return (
            s.encode("latin1")
            .decode("unicode-escape")
            .encode("latin1")
            .decode("utf-8")
            .strip('"')
        )
    except Exception:
        return s


def enrich_profiles_partition(
    batches: Iterable[pa.RecordBatch],
    redis_host: str,
    redis_port: int,
) -> Iterator[pa.RecordBatch]:
    """PyArrow partition iterator for executor-side Redis lookups.

    Design
    ------
    * **Executor-side** - Redis I/O happens on distributed worker nodes, not
      the driver JVM.
    * **Arrow-native alignment** - dedup, re-ordering, and column assembly all
      happen inside ``pyarrow.compute``.  The only data that round-trips through
      Python is the *set of distinct user IDs* needed to build Redis keys; the
      fetched profiles are re-aligned to the original row order with
      ``index_in``/``take`` instead of a per-row Python dict loop.
    * **Per-batch dedup + pipeline** - unique user IDs within each Arrow batch
      are collected, then fetched in a *single* pipelined Redis round-trip.
      There is intentionally no cross-batch in-memory cache: at Spotify/Netflix
      scale (300 M+ users) an unbounded executor-side cache creates severe
      memory pressure.  Redis is designed to serve millions of ops/sec; let it
      do its job.
    * **Resilience** - Redis connection/transport errors are caught gracefully,
      falling back to empty string profile defaults so the stream stays alive.
    """
    r_client = get_executor_redis_client(redis_host, redis_port)
    enriched_fields = ENRICHED_USER_PROFILE_SCHEMA.fieldNames()

    for batch in batches:
        if batch.num_rows == 0:
            yield batch
            continue

        uid_col = batch.column("userId")
        unique_ids = pc.drop_null(pc.unique(uid_col))  # ty: ignore[unresolved-attribute]
        uid_list = unique_ids.to_pylist()

        profiles: list[tuple[str, ...]] = []
        if uid_list:
            try:
                with r_client.pipeline(transaction=False) as pipe:
                    for uid in uid_list:
                        pipe.hmget(f"user:{uid}", *PROFILE_FIELDS)
                    results = pipe.execute()
                profiles = [tuple(v or "" for v in res) for res in results]
            except Exception as exc:
                logger.warning(
                    "Redis enrichment failed for %d IDs (%s). Defaulting to empty.",
                    len(uid_list),
                    exc,
                )
                profiles = [tuple("" for _ in PROFILE_FIELDS) for _ in uid_list]

        # One Arrow array per profile field, plus a trailing "" sentinel row
        # standing in for null user IDs. ``take`` then re-aligns every row in
        # the batch back to its original order.
        sentinel_idx = pa.scalar(len(profiles), type=pa.int32())
        profile_arrays = [
            pa.array([row[i] for row in profiles] + [""], type=pa.string())
            for i in range(len(PROFILE_FIELDS))
        ]
        positions = pc.fill_null(
            pc.index_in(  # ty: ignore[unresolved-attribute]
                uid_col, unique_ids, skip_nulls=True
            ),
            sentinel_idx,
        )
        aligned_arrays = [col.take(positions) for col in profile_arrays]

        new_arrays = [*batch.columns, *aligned_arrays]
        new_names = [*batch.schema.names, *enriched_fields]
        yield pa.RecordBatch.from_arrays(new_arrays, names=new_names)


def project_playback_events_for_clickhouse(df: DataFrame) -> DataFrame:
    return df.select(
        col("event_id"),
        col("userId").alias("user_id"),
        col("artist"),
        col("song"),
        col("duration"),
        col("event_ts"),
        col("sessionId").cast("string").alias("session_id"),
        col("city"),
        col("state"),
        col("enriched_first_name"),
        col("enriched_last_name"),
        col("enriched_gender"),
        col("enriched_city"),
        col("enriched_state"),
        col("enriched_zip"),
        col("song_year"),
        col("artist_location"),
        col("_processing_time"),
    ).fillna(CLICKHOUSE_NULL_DEFAULTS)


def read_kafka_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    max_offsets: int = 10_000,
    starting_offsets: str = "earliest",
) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("maxOffsetsPerTrigger", max_offsets)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_raw_events_with_dlq(
    df: DataFrame,
    schema: StructType,
    topic: str = "listen_events",
) -> tuple[DataFrame, DataFrame]:
    """Parse JSON payload with PERMISSIVE mode, splitting into valid & DLQ DataFrames."""
    parsed_raw = df.select(
        col("value").cast("string").alias("_raw_payload"),
        from_json(
            col("value").cast("string"),
            schema,
            options={
                "mode": "PERMISSIVE",
                "columnNameOfCorruptRecord": "_corrupt_record",
            },
        ).alias("data"),
        col("partition").alias("_kafka_partition"),
        col("offset").alias("_kafka_offset"),
        col("timestamp").alias("_kafka_timestamp"),
    )
    is_corrupt = (
        col("data._corrupt_record").isNotNull()
        | col("data").isNull()
        | col("data.userId").isNull()
        | col("data.ts").isNull()
    )
    # DLQ DataFrame
    dlq_df = (
        parsed_raw.filter(is_corrupt)
        .select(
            col("_raw_payload").alias("raw_payload"),
            lit("ingestion").alias("error_stage"),
            coalesce(
                col("data._corrupt_record"),
                lit("Missing required field(s): userId/ts or unparseable payload"),
            ).alias("error_reason"),
            lit(topic).alias("topic"),
            col("_kafka_partition"),
            col("_kafka_offset"),
            col("_kafka_timestamp"),
            current_timestamp().alias("_processing_time"),
        )
        .withColumn("_processing_date", to_date(col("_processing_time")))
    )
    # filter valid DataFrame
    valid_parsed = parsed_raw.filter(~is_corrupt).select(
        "data.*",
        "_kafka_partition",
        "_kafka_offset",
        "_kafka_timestamp",
    )
    valid_df = (
        valid_parsed.withColumn(
            "event_id",
            sha2(
                concat_ws(
                    "_",
                    col("userId").cast("string"),
                    col("sessionId").cast("string"),
                    col("ts").cast("string"),
                ),
                256,
            ),
        )
        .withColumn("event_ts", (col("ts") / 1000).cast("timestamp"))
        .withColumn("event_date", to_date(col("event_ts")))
        .withColumn("_processing_time", current_timestamp())
        .withColumn("song", decode_escaped_string(col("song")))
        .withColumn("artist", decode_escaped_string(col("artist")))
        .select(
            "artist",
            "song",
            "duration",
            "ts",
            "auth",
            "level",
            "city",
            "zip",
            "state",
            "userAgent",
            "lon",
            "lat",
            "userId",
            "lastName",
            "firstName",
            "gender",
            "registration",
            "sessionId",
            "itemInSession",
            "event_id",
            "event_ts",
            "event_date",
            "_kafka_partition",
            "_kafka_offset",
            "_kafka_timestamp",
            "_processing_time",
        )
    )
    return valid_df, dlq_df
