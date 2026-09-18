"""Physical names and projection contracts.`"""

# ---------------------------------------------------------------------------
# Iceberg table names
# ---------------------------------------------------------------------------
QUARANTINE_TABLE = "quarantine_schema_drift"
DLQ_TABLE = "dlq_events_ingestion"

# ---------------------------------------------------------------------------
# ClickHouse
# ---------------------------------------------------------------------------
CLICKHOUSE_PLAYBACK_EVENTS_TABLE = "silver_playback_events"

# Column -> default used before writing to ClickHouse (single source of truth).
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

# ---------------------------------------------------------------------------
# Redis user-profile hash contract
# ---------------------------------------------------------------------------
# Field names fetched from each ``user:<id>`` hash, ordered to match
# ``ENRICHED_USER_PROFILE_SCHEMA``.
PROFILE_FIELDS: tuple[str, ...] = (
    "first_name",
    "last_name",
    "gender",
    "city",
    "state",
    "zip_code",
)
