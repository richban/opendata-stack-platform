# PRD: Team Ops — Streamify Data Platform

## Introduction

The **Streamify Data Platform** is a real-time music streaming analytics pipeline built on Kafka, Spark Structured Streaming, Apache Iceberg, Polaris REST Catalog, MinIO, and Dagster. It ingests simulated Spotify-like events from Eventsim into cold-storage Iceberg Bronze tables via Spark Connect, then processes those events through Silver (cleaned/deduplicated) and Gold (analytics-ready) batch layers.

**Current state** (as of 2026-03-01):
- ✅ **Bronze streaming layer** — implemented but _not verified_. `bronze_streaming_job` is the only asset. It is a long-running asset that blocks the Dagster run indefinitely until cancelled.
- ⚠️ **Silver batch layer** — planned, not implemented (`silver_listen_events`, `silver_page_view_events`, `silver_auth_events`, `silver_user_sessions`).
- ⚠️ **Gold analytics layer** — planned, not implemented.
- ⚠️ **Sensors / observability** — none exist yet.
- ⚠️ **Automated tests** — none exist yet.

This PRD captures work needed to bring the full pipeline to production quality, following Dagster best practices from the `dagster-expert` skill.

---

## Goals

- Verify the existing Bronze streaming layer end-to-end (data flows from Kafka → Iceberg)
- Implement Silver batch assets: deduplication and sessionization via Spark Connect
- Implement Gold batch assets: aggregation metrics for business analytics
- Add Dagster automation: sensor that detects streaming job health and auto-restarts on failure
- Add Iceberg periodic compaction asset (maintenance)
- Implement Kafka consumer-lag monitoring
- Add asset-level data quality checks (`@dg.asset_check`)
- Achieve full test coverage for all assets using `dg.materialize` with mocked resources
- Ensure all assets conform to Dagster production conventions: `kinds`, `group_name`, `owners`, `tags`, `MaterializeResult` with metadata

---

## User Stories

### US-001: Verify Bronze Streaming Layer End-to-End

**Description:** As a data engineer, I want to verify that the existing `bronze_streaming_job` asset correctly writes events from all three Kafka topics into their respective Iceberg tables, so that I can trust the Bronze layer as a foundation for Silver/Gold processing.

**Acceptance Criteria:**
- [ ] Standing up the full docker-compose stack succeeds without errors
- [ ] `polaris-init` container creates catalog, namespace, principal, and writes credentials without error
- [ ] `bronze_streaming_job` materialises successfully in the Dagster UI at `http://localhost:3000`
- [ ] Spark UI at `http://localhost:8080` shows three active streaming queries (one per topic)
- [ ] MinIO console at `http://localhost:9001` shows Parquet data files under `{catalog}/streamify/bronze_*/`
- [ ] MinIO shows checkpoint directories under `checkpoints/streaming/{topic}`
- [ ] Polaris console at `http://localhost:3001` shows `bronze_listen_events`, `bronze_page_view_events`, `bronze_auth_events` tables with at least one snapshot
- [ ] Kafdrop at `http://localhost:9002` shows messages in all three topics
- [ ] Cancelling the run from the Dagster UI stops all streaming queries cleanly
- [ ] Re-materialising the asset resumes from the MinIO checkpoint (offset continuity verified via Kafdrop consumer-group lag)
- [ ] `bronze_streaming_job` asset decorator includes: `group_name="bronze"`, `kinds={"spark", "iceberg", "kafka"}`, `owners=["team:team-ops"]`, and returns `MaterializeResult` with metadata (topics started, rows per batch, Spark UI URL)

---

### US-002: Refactor `bronze_streaming_job` Asset Metadata and Observability

**Description:** As a data engineer, I want `bronze_streaming_job` to emit rich Dagster metadata on materialisation so that the asset catalogue shows meaningful information about the streaming run.

**Acceptance Criteria:**
- [ ] Asset decorator includes `group_name="bronze"`, `kinds={"spark", "iceberg", "kafka"}`, `owners=["team:team-ops"]`, `tags={"layer": "bronze", "schedule": "streaming"}`
- [ ] Asset returns `dg.MaterializeResult` with metadata:
  - `topics_started`: list of Kafka topics (MetadataValue.json)
  - `spark_ui_url`: clickable URL to Spark UI (MetadataValue.url)
  - `catalog`: Polaris catalog name (MetadataValue.text)
  - `checkpoint_base`: S3 checkpoint path (MetadataValue.text)
- [ ] `dg check defs` passes after changes
- [ ] `dg list defs` shows the asset with correct metadata
- [ ] Characterisation tests written and passing: `pytest tests/test_bronze_assets.py`

---

### US-003: Streaming Health Sensor — Auto-restart on Failure

**Description:** As a platform engineer, I want a Dagster `@run_failure_sensor` that monitors `bronze_streaming_job` and automatically triggers a new materialisation when the run fails or crashes, so that the streaming pipeline self-heals without manual intervention.

**Acceptance Criteria:**
- [ ] `bronze_restart_sensor` implemented in `defs/sensors.py` using `@dg.run_failure_sensor(monitored_jobs=[bronze_streaming_job_run])`
- [ ] Sensor logs a message with the failure reason before triggering a `RunRequest`
- [ ] Sensor is registered in `defs/definitions.py`
- [ ] `dg check defs` passes
- [ ] `dg list defs` shows the sensor
- [ ] Unit test with `build_run_failure_sensor_context` verifies the sensor yields a `RunRequest`
- [ ] Sensor includes a backoff or minimum-restart-interval guard (configurable via `StreamingJobConfig.restart_min_interval_seconds`, default 60s) to prevent restart loops

---

### US-004: Kafka Consumer-Lag Monitoring Sensor

**Description:** As a platform engineer, I want a Dagster sensor that periodically checks Kafka consumer-group lag for all three streaming topics and emits a `SensorResult` with lag metadata, so that I can detect when the pipeline is falling behind.

**Acceptance Criteria:**
- [ ] `kafka_lag_sensor` implemented in `defs/sensors.py` using `@dg.sensor(minimum_interval_seconds=60)`
- [ ] Sensor connects to Kafka using `KAFKA_BOOTSTRAP_SERVERS` env var via `KafkaAdminClient`
- [ ] Sensor reads consumer group lag per topic per partition
- [ ] Sensor emits `dg.SensorResult` with `dynamic_partitions_requests=[]` and `cursor` (last-checked timestamp) to avoid duplicate alerts
- [ ] Uses `context.log.warning` when total lag across any topic exceeds configurable threshold (`StreamingJobConfig.kafka_lag_warn_threshold`, default 10000)
- [ ] Sensor metadata logged per run in Dagster event log
- [ ] `dg check defs` passes
- [ ] Unit test validates sensor logic with a mocked `KafkaAdminClient`

---

### US-005: Iceberg Table Compaction Asset

**Description:** As a data engineer, I want a scheduled Dagster asset that runs Iceberg `rewrite_data_files` on all three Bronze tables, so that small Parquet files produced by 30-second micro-batches are periodically compacted to improve read performance.

**Acceptance Criteria:**
- [ ] `bronze_compaction` asset implemented in `defs/assets.py` (or `defs/maintenance_assets.py`)
- [ ] Asset uses `SparkConnectResource` to run `CALL {catalog}.system.rewrite_data_files(table => 'streamify.bronze_{topic}')` for each topic via Spark SQL
- [ ] Asset includes `group_name="bronze_maintenance"`, `kinds={"spark", "iceberg"}`, `owners=["team:team-ops"]`
- [ ] Asset depends on `bronze_streaming_job` via `deps=[AssetKey("bronze_streaming_job")]`
- [ ] `ScheduleDefinition` added: `bronze_compaction_schedule` using cron `"0 2 * * *"` (daily at 02:00 UTC)
- [ ] Asset returns `MaterializeResult` with metadata: files rewritten per table, bytes written
- [ ] `dg check defs` passes
- [ ] Unit test verifies asset executes compaction SQL for all three topics

---

### US-006: Silver Layer — Deduplicated Listen Events Asset

**Description:** As a data engineer, I want a `silver_listen_events` Dagster asset that reads from `bronze_listen_events` and writes deduplicated records to `silver_listen_events` in the Polaris catalog, so that downstream analytics work on clean, non-duplicate data.

**Acceptance Criteria:**
- [ ] `silver_listen_events` asset implemented in `defs/silver_assets.py`
- [ ] Asset uses `SparkConnectResource` to run a Spark batch read from `bronze_listen_events`
- [ ] Deduplication uses `event_id` as the unique key (SHA-256 hash already present in Bronze)
- [ ] Deduplication uses `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY _processing_time DESC)` pattern to keep the latest record
- [ ] Result written to `{catalog}.streamify.silver_listen_events` in Iceberg with `mode("overwrite")` or incremental merge strategy (prefer merge for production)
- [ ] Asset partitioned by `event_date` (`DailyPartitionsDefinition` or `HourlyPartitionsDefinition`)
- [ ] Asset includes `group_name="silver"`, `kinds={"spark", "iceberg"}`, `owners=["team:team-ops"]`, `tags={"layer": "silver", "topic": "listen_events"}`
- [ ] Asset returns `MaterializeResult` with metadata: `input_rows`, `output_rows`, `duplicate_rows_removed`
- [ ] `dg check defs` passes, asset visible in `dg list defs`
- [ ] Unit tests pass: `pytest tests/test_silver_assets.py`

---

### US-007: Silver Layer — Deduplicated Page View Events Asset

**Description:** As a data engineer, I want a `silver_page_view_events` asset that deduplicates Bronze page-view events by `event_id`, so that page-level analytics are accurate.

**Acceptance Criteria:**
- [ ] `silver_page_view_events` asset implemented in `defs/silver_assets.py` following the same pattern as `silver_listen_events` (US-006)
- [ ] Deduplication by `event_id`
- [ ] Partitioned by `event_date`
- [ ] Written to `{catalog}.streamify.silver_page_view_events`
- [ ] Asset includes `group_name="silver"`, `kinds={"spark", "iceberg"}`, `owners=["team:team-ops"]`, `tags={"layer": "silver", "topic": "page_view_events"}`
- [ ] Returns `MaterializeResult` with `input_rows`, `output_rows`, `duplicate_rows_removed`
- [ ] `dg check defs` passes
- [ ] Unit tests pass

---

### US-008: Silver Layer — Deduplicated Auth Events Asset

**Description:** As a data engineer, I want a `silver_auth_events` asset that deduplicates Bronze auth events by `event_id`, so that authentication analytics (login success rate, cancellation rate) are based on clean data.

**Acceptance Criteria:**
- [ ] `silver_auth_events` asset implemented in `defs/silver_assets.py` following the same pattern as `silver_listen_events` (US-006)
- [ ] Deduplication by `event_id`
- [ ] Partitioned by `event_date`
- [ ] Written to `{catalog}.streamify.silver_auth_events`
- [ ] Asset includes `group_name="silver"`, `kinds={"spark", "iceberg"}`, `owners=["team:team-ops"]`, `tags={"layer": "silver", "topic": "auth_events"}`
- [ ] Returns `MaterializeResult` with `input_rows`, `output_rows`, `duplicate_rows_removed`
- [ ] `dg check defs` passes
- [ ] Unit tests pass

---

### US-009: Silver Layer — User Session Reconstruction Asset

**Description:** As a data engineer, I want a `silver_user_sessions` asset that reconstructs user sessions from `silver_listen_events` and `silver_page_view_events` using a 30-minute inactivity timeout, so that session-level metrics (session length, tracks per session, page depth) are available for Gold analytics.

**Acceptance Criteria:**
- [ ] `silver_user_sessions` asset implemented in `defs/silver_assets.py`
- [ ] Asset depends on `silver_listen_events` and `silver_page_view_events` via function parameters
- [ ] Session boundary defined as: gap > 30 minutes between consecutive events for same `userId`
- [ ] Session ID generated as `SHA-256(userId_sessionId_session_start_ts)`
- [ ] Output schema includes: `session_id`, `user_id`, `session_start`, `session_end`, `session_duration_seconds`, `track_count`, `page_count`, `level`
- [ ] Written to `{catalog}.streamify.silver_user_sessions` in Iceberg, partitioned by `session_date`
- [ ] Asset includes `group_name="silver"`, `kinds={"spark", "iceberg"}`, `owners=["team:team-ops"]`
- [ ] Returns `MaterializeResult` with `session_count`, `avg_session_duration_seconds`, `avg_tracks_per_session`
- [ ] `dg check defs` passes
- [ ] Unit tests pass with mock Spark data

---

### US-010: Silver Layer Data Quality Checks

**Description:** As a data engineer, I want Dagster asset checks (`@dg.asset_check`) on all Silver assets to verify data quality after each materialisation, so that quality issues are caught at the pipeline layer before propagating to Gold.

**Acceptance Criteria:**
- [ ] `silver_listen_events_not_empty` check: fails if output table has 0 rows
- [ ] `silver_listen_events_no_nulls` check: fails if `event_id` or `userId` is null in > 0.01% of rows
- [ ] `silver_listen_events_no_duplicates` check: fails if `COUNT(DISTINCT event_id) < COUNT(*)`
- [ ] Same three checks implemented for `silver_page_view_events` and `silver_auth_events`
- [ ] `silver_user_sessions_not_empty` check: fails if session count is 0
- [ ] `silver_user_sessions_valid_duration` check: fails if any `session_duration_seconds < 0`
- [ ] All checks are registered in `defs/definitions.py`
- [ ] All checks return `dg.AssetCheckResult` with `metadata` (row count, null count, duplicate count)
- [ ] `dg check defs` passes
- [ ] `pytest tests/test_silver_checks.py` passes

---

### US-011: Silver Batch Schedule

**Description:** As a data engineer, I want a daily schedule that materialises all Silver assets in dependency order (listen/page_view/auth → sessions), so that Silver data is refreshed once per day without manual intervention.

**Acceptance Criteria:**
- [ ] `silver_daily_schedule` defined using `dg.ScheduleDefinition` in `defs/schedules.py`
- [ ] Cron expression: `"0 4 * * *"` (04:00 UTC daily)
- [ ] Schedule targets a `define_asset_job` (`silver_batch_job`) using `AssetSelection.groups("silver").downstream()`
- [ ] Schedule registered in `defs/definitions.py`
- [ ] `dg check defs` passes, schedule visible in `dg list defs`

---

### US-012: Gold Layer — Top Tracks and Artists Daily Aggregation

**Description:** As an analyst, I want a `gold_top_tracks` and `gold_top_artists` asset that aggregates daily play counts from `silver_listen_events`, so that I can query the most popular content for any given day.

**Acceptance Criteria:**
- [ ] `gold_top_tracks` and `gold_top_artists` assets implemented in `defs/gold_assets.py`
- [ ] Both depend on `silver_listen_events` via function parameter
- [ ] `gold_top_tracks`: groups by `(event_date, song, artist)`, aggregates `play_count = COUNT(*)`, `unique_listeners = COUNT(DISTINCT userId)`, `avg_duration = AVG(duration)`
- [ ] `gold_top_artists`: groups by `(event_date, artist)`, aggregates `play_count`, `unique_listeners`
- [ ] Written to `{catalog}.streamify.gold_top_tracks` and `gold_top_artists`, partitioned by `event_date`
- [ ] Both assets include `group_name="gold"`, `kinds={"spark", "iceberg"}`, `owners=["team:team-ops"]`, `tags={"layer": "gold"}`
- [ ] Return `MaterializeResult` with `output_rows` and `event_date` processed
- [ ] `dg check defs` passes
- [ ] Unit tests pass

---

### US-013: Gold Layer — DAU/MAU Metrics Asset

**Description:** As an analyst, I want a `gold_dau_mau` asset that computes Daily Active Users and Monthly Active Users from `silver_listen_events`, so that I can track user engagement trends over time.

**Acceptance Criteria:**
- [ ] `gold_dau_mau` asset implemented in `defs/gold_assets.py`
- [ ] Depends on `silver_listen_events` via function parameter
- [ ] DAU: `COUNT(DISTINCT userId)` grouped by `event_date`
- [ ] MAU: `COUNT(DISTINCT userId)` grouped by `year-month` (derived from `event_date`)
- [ ] Written to `{catalog}.streamify.gold_dau_mau`, partitioned by `event_date`
- [ ] Asset includes `group_name="gold"`, `kinds={"spark", "iceberg"}`, `owners=["team:team-ops"]`
- [ ] Returns `MaterializeResult` with `dau`, `event_date`, `mau_month`
- [ ] `dg check defs` passes
- [ ] Unit tests pass

---

### US-014: Gold Layer — User Conversion Funnel Asset

**Description:** As an analyst, I want a `gold_user_conversion_funnel` asset that models the conversion funnel (Guest → Registered → Paid) from `silver_auth_events` and `silver_listen_events`, so that I can measure subscription conversion rates.

**Acceptance Criteria:**
- [ ] `gold_user_conversion_funnel` asset implemented in `defs/gold_assets.py`
- [ ] Depends on `silver_auth_events` and `silver_listen_events`
- [ ] Funnel stages derived from `level` field: `"free"` (registered free), `"paid"` (subscription), auth failures with `success = "false"` as bounced logins
- [ ] Produces daily cohort-level funnel counts: `(event_date, stage, user_count, percentage_of_prior_stage)`
- [ ] Written to `{catalog}.streamify.gold_user_conversion_funnel`
- [ ] Asset includes `group_name="gold"`, `kinds={"spark", "iceberg"}`, `owners=["team:team-ops"]`
- [ ] Returns `MaterializeResult` with `event_date`, `free_users`, `paid_users`, `conversion_rate`
- [ ] `dg check defs` passes
- [ ] Unit tests pass

---

### US-015: Gold Layer — User Churn Analysis Asset

**Description:** As an analyst, I want a `gold_user_churn` asset that identifies churned users from `silver_auth_events` based on `auth = 'Cancelled'` events, so that I can monitor churn rate and user retention over time.

**Acceptance Criteria:**
- [ ] `gold_user_churn` asset implemented in `defs/gold_assets.py`
- [ ] Depends on `silver_auth_events`
- [ ] Churned user: `userId` with at least one event where `success = "false"` at `level = "cancelled"` (or auth event type indicating cancellation)
- [ ] Output: `(event_date, churned_user_count, churn_rate_pct)` per day
- [ ] Written to `{catalog}.streamify.gold_user_churn`
- [ ] Asset includes `group_name="gold"`, `kinds={"spark", "iceberg"}`, `owners=["team:team-ops"]`
- [ ] Returns `MaterializeResult` with `event_date`, `churned_user_count`, `churn_rate_pct`
- [ ] `dg check defs` passes
- [ ] Unit tests pass

---

### US-016: Gold Layer Batch Schedule

**Description:** As a data engineer, I want a daily schedule that materialises all Gold assets after Silver assets complete, so that analytics tables are refreshed automatically every day.

**Acceptance Criteria:**
- [ ] `gold_daily_schedule` defined using `dg.ScheduleDefinition` in `defs/schedules.py`
- [ ] Cron expression: `"0 6 * * *"` (06:00 UTC daily — 2 hours after Silver)
- [ ] Schedule targets `gold_batch_job` using `AssetSelection.groups("gold")`
- [ ] Schedule registered in `defs/definitions.py`
- [ ] `dg check defs` passes

---

### US-017: End-to-End Integration Test Suite

**Description:** As a developer, I want a comprehensive test suite that validates all asset definitions load correctly and key assets can be materialised with mocked resources, so that CI catches regressions before deployment.

**Acceptance Criteria:**
- [ ] `tests/` directory created with `conftest.py` providing fixture mocks for `SparkConnectResource`, `S3Resource`, `StreamingJobConfig`
- [ ] `tests/test_bronze_assets.py`: tests `bronze_streaming_job` mock materialisation
- [ ] `tests/test_silver_assets.py`: tests `silver_listen_events`, `silver_page_view_events`, `silver_auth_events`, `silver_user_sessions` mock materialisation
- [ ] `tests/test_gold_assets.py`: tests all Gold assets with mock Spark outputs
- [ ] `tests/test_sensors.py`: tests `bronze_restart_sensor` and `kafka_lag_sensor` with mock contexts
- [ ] `tests/test_checks.py`: tests all Silver layer `asset_check` definitions
- [ ] All tests pass: `pytest tests/ -v`
- [ ] `dg check defs` passes

---

## Functional Requirements

- **FR-1**: `bronze_streaming_job` must emit `MaterializeResult` with at minimum: `topics_started`, `spark_ui_url`, `catalog`, `checkpoint_base` metadata fields.
- **FR-2**: A `@run_failure_sensor` named `bronze_restart_sensor` must monitor `bronze_streaming_job` failures and trigger re-materialisation with a minimum restart interval guard.
- **FR-3**: A `@sensor` named `kafka_lag_sensor` must poll Kafka consumer-group lag with a minimum interval of 60 seconds and log warnings when lag exceeds `StreamingJobConfig.kafka_lag_warn_threshold`.
- **FR-4**: A `bronze_compaction` asset must execute `CALL system.rewrite_data_files` for all three Bronze tables on a daily cron schedule.
- **FR-5**: Silver assets (`silver_listen_events`, `silver_page_view_events`, `silver_auth_events`) must deduplicate by `event_id` using window functions; result counts must be reported as `MaterializeResult` metadata.
- **FR-6**: `silver_user_sessions` must reconstruct sessions with 30-minute inactivity timeout, deriving `session_id`, `session_start`, `session_end`, `session_duration_seconds`, `track_count`, `page_count`.
- **FR-7**: All Silver assets must have corresponding `@dg.asset_check` definitions for: non-empty output, null check on key fields, and duplicate check on `event_id`.
- **FR-8**: All Gold assets must depend on Silver assets via function parameters (Dagster asset dependency graph).
- **FR-9**: Silver batch job scheduled at `0 4 * * *`; Gold batch job scheduled at `0 6 * * *`.
- **FR-10**: All assets must include Dagster asset metadata decorators: `group_name`, `kinds`, `owners`, `tags`.
- **FR-11**: All resources must use `EnvVar(...)` for credentials — no hardcoded secrets.
- **FR-12**: All new assets must be validated with `dg check defs` and appear in `dg list defs` output.
- **FR-13**: Full test suite must pass with `pytest tests/ -v`.

---

## Non-Goals (Out of Scope)

- **No SQLMesh integration** in this PRD — potential future enhancement noted in README but out of scope here.
- **No Dagster Pipes / spark-submit** — all Spark interaction continues via Spark Connect.
- **No Kafka Schema Registry** — JSON schema validation handled by PySpark schema in `schemas.py`.
- **No external alerting** (Slack, PagerDuty) — sensors log warnings only; alerting integration is a future story.
- **No UI/dashboards** — Iceberg tables are the output; BI tool integration is out of scope.
- **No cloud deployment / Kubernetes** — development targets docker-compose only.
- **No Gold layer real-time streaming** — Gold layer is batch-only.
- **No automated Kafka topic creation** — topics are created by Eventsim.

---

## Technical Considerations

### Dagster Asset Conventions (from `dagster-expert` skill)

All assets must follow production patterns:
- Use **noun-based names** (e.g., `silver_listen_events`, not `process_listen_events`)
- Always include `kinds={"spark", "iceberg"}` (and `"kafka"` for streaming)
- Use `ConfigurableResource` and `EnvVar(...)` for all external service config
- Return `dg.MaterializeResult` with metadata for observability
- Use `group_name` to organise by layer: `"bronze"`, `"bronze_maintenance"`, `"silver"`, `"gold"`
- Use `deps=[AssetKey(...)]` for cross-asset deps that don't pass data directly (e.g., compaction → bronze)

### SparkConnectResource (existing)

`SparkConnectResource` in `defs/resources.py` handles OAuth2 auth to Polaris and is already wired up. Silver/Gold assets should accept it as a function parameter.

### Partitioning Strategy

Silver and Gold assets should use `DailyPartitionsDefinition(start_date="2024-01-01")` to enable selective backfill and incremental processing. Initial implementation may use full overwrite until incremental merge is confirmed working against Iceberg on Polaris.

### Iceberg Write Pattern

Silver: prefer `MERGE INTO` for incremental deduplication; use `mode("overwrite")` for initial implementation. Gold: use `mode("overwrite").partitionOverwriteMode("dynamic")` to replace only the affected partitions.

### Environment Variables

All new resources use existing env vars from the README. New additions:
- `KAFKA_LAG_WARN_THRESHOLD` (default: 10000)
- `STREAMING_RESTART_MIN_INTERVAL_SECONDS` (default: 60)

---

## Success Metrics

- Bronze layer verified: Parquet files appear in MinIO within 60 seconds of asset materialisation
- Silver deduplication accuracy: `duplicate_rows_removed` metadata > 0 for a known-replay scenario; `event_id` uniqueness check passes
- Silver session reconstruction: `avg_session_duration_seconds` between 60s and 3600s for a typical simulated user session
- Gold assets: `gold_top_tracks` has at least 1 row per `event_date` with backfill data available
- All `pytest tests/ -v` tests pass (0 failures)
- `dg check defs` passes with 0 errors
- No Dagster run failures in a 24-hour continuous streaming window

---

## Open Questions

1. **Iceberg merge vs overwrite for Silver**: Should `silver_listen_events` use `MERGE INTO` (incremental, append-safe) or `mode("overwrite")` (simpler, full-refresh per partition)? Merge is preferred for streaming correctness but requires more testing against Polaris.
    - Let's use overwrite for now and migrate to merge if needed.
2. **Partitioning granularity for Silver**: Daily partitions are sufficient for batch analytics, but hourly partitions reduce per-run data volume. Recommendation: start with daily, migrate to hourly if SLA requires.
    - Let's use daily for now and migrate to hourly if needed.
3. **Sensor restart guard**: Should `bronze_restart_sensor` use Dagster's built-in `minimum_interval_seconds` on the sensor, or implement a cursor-based backoff? Cursor-based is more flexible.
    - Let's use cursor-based for now and migrate to minimum_interval_seconds if needed.
4. **Polaris multi-namespace isolation**: Should Gold tables live in a separate namespace (`streamify_gold`) or the same `streamify` namespace? Keeping one namespace simplifies IAM but makes table names longer.
    - Let's use the same namespace for now and migrate to a separate namespace if needed.
5. **Testing streaming assets**: `bronze_streaming_job` uses `session.streams.awaitAnyTermination()` which blocks forever — should the test mock the `SparkSession` entirely, or use a bounded stream with `trigger(once=True)`?
    - Let's mock the SparkSession for now and migrate to a bounded stream if needed.
6. **Data quality checks**: Should we implement data quality checks for the Silver and Gold layers?
    - Let's implement data quality checks for the Silver and Gold layers.
    - Let's use the `@dg.asset_check` decorator to implement data quality checks for the Silver and Gold layers.
    - We should implement some custom data quality checks with alerting and notifications. It can be a just a simple logging to a file. But let's create a table to store the results of the checks and alert on the results. Something very minimal and simple.