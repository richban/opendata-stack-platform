"""Confluent Schema Registry access and schema field utilities.

This module resolves schema ids to writer schemas (for Avro/Protobuf payloads)
and provides the field utilities used by the classification layer. The actual
drift classification / routing lives in :mod:`streamify.classification`.
"""

import hashlib
import json
import logging

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from cachetools import LRUCache

from streamify.wire import WireFormat

logger = logging.getLogger(__name__)

DEFAULT_CACHE_SIZE = 256


@dataclass(frozen=True)
class StreamSchemaConfig:
    """Composition-root schema settings: per-topic wire format."""

    default_wire_format: WireFormat = WireFormat.JSON
    wire_format_by_topic: Mapping[str, WireFormat] = field(default_factory=dict)

    def wire_format_for(self, topic: str) -> WireFormat:
        """Wire format for ``topic`` (``listen_events`` JSON, ``user_profiles`` Avro)."""
        return self.wire_format_by_topic.get(topic, self.default_wire_format)


@dataclass(frozen=True)
class RegisteredSchema:
    """A schema resolved from the registry (or a bundled fallback)."""

    schema_id: int
    schema_str: str
    subject: str | None = None
    version: int | None = None
    schema_type: str = "AVRO"


def field_fingerprint(fields: Any) -> str:
    """Stable fingerprint of a field-name collection, order-independent."""
    normalized = sorted(str(field) for field in fields)
    digest = hashlib.sha256("\n".join(normalized).encode("utf-8"))
    return digest.hexdigest()


def avro_field_names(schema_str: str) -> tuple[str, ...]:
    """Extract top-level record field names from an Avro schema string."""
    try:
        parsed = json.loads(schema_str)
    except (TypeError, ValueError):
        logger.warning("Could not parse Avro schema for field extraction.")
        return ()
    if not isinstance(parsed, dict):
        return ()
    fields = parsed.get("fields")
    if not isinstance(fields, list):
        return ()
    return tuple(
        str(field["name"])
        for field in fields
        if isinstance(field, dict) and "name" in field
    )


def json_top_level_keys(payload: bytes | str) -> tuple[str, ...]:
    """Sorted top-level keys of a JSON object payload.

    Raises ``ValueError`` when the payload is not a JSON object.
    """
    text = (
        payload.decode("utf-8", errors="replace")
        if isinstance(payload, bytes)
        else payload
    )
    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise ValueError("payload is not a JSON object")
    return tuple(sorted(str(key) for key in parsed))


class ConfluentSchemaResolver:
    """Resolves schema ids against a live Confluent Schema Registry.

    The client is created lazily and all lookups are cached; a registry outage
    degrades to ``None`` rather than crashing the stream.
    """

    def __init__(
        self,
        url: str,
        *,
        cache_size: int = DEFAULT_CACHE_SIZE,
        client: Any | None = None,
    ) -> None:
        self.url = url
        self.schemas_by_id: LRUCache = LRUCache(maxsize=cache_size)
        self.latest_by_subject: LRUCache = LRUCache(maxsize=cache_size)
        self.registry_client = client

    @property
    def client(self) -> Any:
        if self.registry_client is None:
            from confluent_kafka.schema_registry import (  # noqa: PLC0415
                SchemaRegistryClient,
            )

            self.registry_client = SchemaRegistryClient({"url": self.url})
        return self.registry_client

    def get_by_id(self, schema_id: int) -> RegisteredSchema | None:
        if schema_id in self.schemas_by_id:
            return self.schemas_by_id[schema_id]
        try:
            schema = self.client.get_schema(schema_id)
        except Exception as exc:  # registry outage or unknown id
            logger.warning("Schema id %s could not be resolved: %s", schema_id, exc)
            return None
        resolved = RegisteredSchema(
            schema_id=schema_id,
            schema_str=schema.schema_str,
            schema_type=str(getattr(schema, "schema_type", "AVRO")),
        )
        self.schemas_by_id[schema_id] = resolved
        return resolved

    def get_latest(self, subject: str) -> RegisteredSchema | None:
        if subject in self.latest_by_subject:
            return self.latest_by_subject[subject]
        try:
            latest = self.client.get_latest_version(subject)
        except Exception as exc:
            logger.warning("Latest schema for subject %s unavailable: %s", subject, exc)
            return None
        resolved = RegisteredSchema(
            schema_id=latest.schema_id,
            schema_str=latest.schema.schema_str,
            subject=latest.subject,
            version=latest.version,
            schema_type=str(getattr(latest.schema, "schema_type", "AVRO")),
        )
        self.latest_by_subject[subject] = resolved
        return resolved
