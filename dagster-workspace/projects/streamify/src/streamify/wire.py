"""Kafka wire-format framing for Streamify ingestion.

Supports two payload encodings:

* ``WireFormat.JSON`` - the legacy EventSim producer emits plain UTF-8 JSON.
  There is no header, so no schema id can be recovered from the payload.
* ``WireFormat.AVRO`` / ``WireFormat.PROTOBUF`` - Confluent Schema Registry
  framing::

      byte 0      magic byte (0x00)
      bytes 1-4   schema id (big-endian unsigned 32-bit)
      bytes 5..   serialized payload

The pure functions here are deliberately free of ``SparkSession`` so they can
be unit-tested without a cluster; the ``*_column`` helpers emit native Spark
expressions (no Python UDF) so the hot ingestion path stays on the JVM.
"""

import logging

from dataclasses import dataclass
from enum import Enum

from pyspark.sql import Column
from pyspark.sql.functions import conv, hex as spark_hex, length, substring, when

logger = logging.getLogger(__name__)

CONFLUENT_MAGIC_BYTE = 0
CONFLUENT_HEADER_LENGTH = 5
SCHEMA_ID_HEX_LENGTH = 8


class WireFormat(str, Enum):  # noqa: UP042 - keep Python 3.10 compatibility
    """Supported Kafka payload encodings."""

    JSON = "json"
    AVRO = "avro"
    PROTOBUF = "protobuf"


def is_confluent_encoded(wire_format: WireFormat) -> bool:
    return wire_format in (WireFormat.AVRO, WireFormat.PROTOBUF)


class MalformedWireError(ValueError):
    """Raised when a payload cannot be framed by the configured wire format."""


@dataclass(frozen=True)
class WireFrame:
    """A decoded wire envelope: optional schema id plus the bare payload."""

    schema_id: int | None
    payload: bytes


def decode_wire_frame(
    value: bytes | bytearray | memoryview | None,
    *,
    wire_format: WireFormat = WireFormat.AVRO,
    magic_byte: int = CONFLUENT_MAGIC_BYTE,
) -> WireFrame:
    """Split a Kafka ``value`` into its schema id and payload.

    Raises ``MalformedWireError`` for null, empty, truncated, or
    incorrectly-framed payloads so callers can route them to the DLQ rather
    than crashing the stream.
    """
    if value is None:
        raise MalformedWireError("null payload")

    data = bytes(value)
    if not data:
        raise MalformedWireError("empty payload")

    if not is_confluent_encoded(wire_format):
        return WireFrame(schema_id=None, payload=data)

    if len(data) < CONFLUENT_HEADER_LENGTH:
        raise MalformedWireError(
            f"payload of {len(data)} bytes is shorter than the "
            f"{CONFLUENT_HEADER_LENGTH}-byte Confluent header"
        )
    if data[0] != magic_byte:
        raise MalformedWireError(
            f"unexpected Confluent magic byte {data[0]!r} (expected {magic_byte})"
        )

    schema_id = int.from_bytes(data[1:CONFLUENT_HEADER_LENGTH], "big")
    return WireFrame(schema_id=schema_id, payload=data[CONFLUENT_HEADER_LENGTH:])


def is_confluent_framed(value: Column) -> Column:
    """Native expression: magic byte equals ``0x00``."""
    return spark_hex(substring(value, 1, 1)) == format(CONFLUENT_MAGIC_BYTE, "02x")


def schema_id_column(value: Column) -> Column:
    """Native expression extracting the big-endian schema id, or null.

    Reads the 8 hex characters covering header bytes 1-4 directly, avoiding a
    Python UDF on the ingestion path.
    """
    raw_id = substring(spark_hex(value), 3, SCHEMA_ID_HEX_LENGTH)
    parsed = conv(raw_id, 16, 10).cast("int")
    return when(is_confluent_framed(value), parsed)


def payload_column(value: Column) -> Column:
    """Native expression stripping the 5-byte Confluent header from ``value``."""
    return substring(value, CONFLUENT_HEADER_LENGTH + 1, length(value))
