from collections.abc import Iterable, Iterator
import logging

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


def string_decode_fn(s: str, encoding: str = "utf-8") -> str:
    """Decode unicode/octal-escaped strings (e.g. artist/song names from eventsim)."""
    if s:
        try:
            return (
                s.encode("latin1")
                .decode("unicode-escape")
                .encode("latin1")
                .decode(encoding)
                .strip('"')
            )
        except Exception:
            return s
    return s


def string_decode_vec(series: pd.Series) -> pd.Series:  # type: ignore[type-arg]
    """Vectorised wrapper around ``_string_decode_fn``."""
    return series.apply(string_decode_fn)  # type: ignore[return-value]  # ty: ignore[invalid-return-type]


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
