"""Schema-drift classification (data-quality routing).

This is **not** on the streaming fast path. The pipeline is the only consumer,
so the contract is its own schema (``SCHEMAS[topic]``). A record is classified
by comparing its *observed* fields to the contract's *expected* fields:

* **JSON** (no schema id): observed fields are the payload's top-level keys.
* **Avro/Protobuf** (Confluent-framed): observed fields would come from the
  writer schema resolved via the registry by ``schema_id``; the field-level
  check is wired when the registry decode lands, so for now an id is presumed
  ready.

Result: READY (fields match), DRIFT (unknown/missing fields), or MALFORMED
(undecodable). Both produce a :class:`SchemaClassification` so routing is
uniform.

Only the batch silver transform (``streamify.transformations.silver``) should
import this: the stream writes bronze + ClickHouse and never routes drift.
"""

import logging

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, to_date, udf, when
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from streamify.schema_registry import (
    field_fingerprint,
    json_top_level_keys,
)
from streamify.wire import WireFormat

if TYPE_CHECKING:
    from pyspark.sql.udf import UserDefinedFunctionLike

logger = logging.getLogger(__name__)


SCHEMA_CLASSIFICATION_SCHEMA = StructType(
    [
        StructField("status", StringType(), True),
        StructField("reason", StringType(), True),
        StructField("observed_fingerprint", StringType(), True),
        StructField("expected_fingerprint", StringType(), True),
    ]
)


class SchemaStatus(str, Enum):  # noqa: UP042 - keep Python 3.10 compatibility
    """How a payload relates to the deployed consumer contract."""

    READY = "ready"
    DRIFT = "schema_drift"
    MALFORMED = "malformed"


@dataclass(frozen=True)
class SchemaClassification:
    """The verdict for a single payload, with a human-readable reason."""

    status: SchemaStatus
    reason: str
    observed_fingerprint: str | None = None
    expected_fingerprint: str | None = None


def classify_json_payload(
    payload: bytes | str,
    expected_fields: tuple[str, ...] | list[str],
) -> SchemaClassification:
    """Classify a headerless JSON payload by comparing top-level key sets."""
    try:
        observed = json_top_level_keys(payload)
    except ValueError as exc:
        return SchemaClassification(
            status=SchemaStatus.MALFORMED,
            reason=f"Payload is not a JSON object: {exc}",
        )

    expected = tuple(sorted(str(field) for field in expected_fields))
    expected_fp = field_fingerprint(expected)
    observed_fp = field_fingerprint(observed)

    if observed_fp == expected_fp:
        return SchemaClassification(
            status=SchemaStatus.READY,
            reason="Payload keys match the consumer contract.",
            observed_fingerprint=observed_fp,
            expected_fingerprint=expected_fp,
        )

    missing = sorted(set(expected) - set(observed))
    unknown = sorted(set(observed) - set(expected))
    parts = []
    if unknown:
        parts.append(f"unknown keys {unknown}")
    if missing:
        parts.append(f"missing keys {missing}")
    return SchemaClassification(
        status=SchemaStatus.DRIFT,
        reason="Schema churn detected: " + "; ".join(parts) + ".",
        observed_fingerprint=observed_fp,
        expected_fingerprint=expected_fp,
    )


def classify_json_record(
    payload: bytes | None,
    expected_fields: tuple[str, ...],
) -> tuple[str, str, str | None, str | None]:
    if payload is None:
        return (SchemaStatus.MALFORMED.value, "null payload", None, None)
    classification = classify_json_payload(bytes(payload), expected_fields)
    return (
        classification.status.value,
        classification.reason,
        classification.observed_fingerprint,
        classification.expected_fingerprint,
    )


def classify_json_udf(expected_fields: tuple[str, ...]) -> "UserDefinedFunctionLike":
    """Spark UDF classifying a JSON payload's keys against the contract."""
    expected = tuple(expected_fields)
    return udf(
        lambda payload: classify_json_record(payload, expected),
        returnType=SCHEMA_CLASSIFICATION_SCHEMA,
    )


def classify_raw_events(
    df: DataFrame,
    *,
    topic: str,
    wire_format: WireFormat = WireFormat.JSON,
    expected_fields: tuple[str, ...] = (),
    processing_time_column: str = "",
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Split a raw-landing frame into ready / schema-drift / malformed frames.

    The contract is the pipeline's own schema (``expected_fields``): a record is
    READY when its observed fields match, DRIFT when they differ (unknown or
    missing fields), MALFORMED when it cannot be parsed at all. Drift is
    *retryable* (replay after the contract is widened); malformed is permanent.

    ``processing_time_column`` pins ``_processing_time`` to an existing column
    (e.g. ``_ingest_time``) instead of wall-clock time, so batch re-runs are
    deterministic and idempotent.
    """
    processing_time = (
        col(processing_time_column) if processing_time_column else current_timestamp()
    )
    base = df.withColumn("_processing_time", processing_time).withColumn(
        "_processing_date", to_date(col("_processing_time"))
    )

    if wire_format is WireFormat.JSON:
        classification = classify_json_udf(expected_fields)(col("raw_value"))
        classified = base.select(
            "*",
            classification["status"].alias("schema_status"),
            classification["reason"].alias("schema_reason"),
            classification["observed_fingerprint"].alias("observed_fingerprint"),
            classification["expected_fingerprint"].alias("expected_fingerprint"),
            lit(None).cast(IntegerType()).alias("observed_schema_id"),
        )
    else:
        # Confluent-framed payloads carry a schema id but no cheap way to read
        # its fields without the registry, so field-level drift for Avro is
        # deferred to the registry-level decode; for now an id is presumed ready.
        classified = base.select(
            "*",
            when(col("schema_id").isNull(), SchemaStatus.MALFORMED.value)
            .otherwise(SchemaStatus.READY.value)
            .alias("schema_status"),
            when(
                col("schema_id").isNull(),
                lit("No Confluent schema id could be extracted from the payload."),
            )
            .otherwise(lit("Schema id resolved (field-level check pending)."))
            .alias("schema_reason"),
            lit(None).cast(StringType()).alias("observed_fingerprint"),
            lit(None).cast(StringType()).alias("expected_fingerprint"),
            col("schema_id").alias("observed_schema_id"),
        )

    drift_df = classified.filter(col("schema_status") == SchemaStatus.DRIFT.value).select(
        col("raw_value"),
        col("observed_schema_id"),
        col("observed_fingerprint"),
        col("expected_fingerprint"),
        lit("schema_drift").alias("error_stage"),
        col("schema_reason").alias("error_reason"),
        lit(topic).alias("topic"),
        col("_kafka_partition"),
        col("_kafka_offset"),
        col("_kafka_timestamp"),
        col("_processing_time"),
        col("_processing_date"),
    )

    dlq_df = classified.filter(
        col("schema_status") == SchemaStatus.MALFORMED.value
    ).select(
        col("raw_value").cast("string").alias("raw_payload"),
        lit("ingestion").alias("error_stage"),
        col("schema_reason").alias("error_reason"),
        lit(topic).alias("topic"),
        col("_kafka_partition"),
        col("_kafka_offset"),
        col("_kafka_timestamp"),
        col("_processing_time"),
        col("_processing_date"),
    )

    ready_df = classified.filter(col("schema_status") == SchemaStatus.READY.value).drop(
        "schema_status",
        "schema_reason",
        "observed_fingerprint",
        "expected_fingerprint",
        "observed_schema_id",
    )

    return ready_df, drift_df, dlq_df
