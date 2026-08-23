# Streamify — MVP Production-Readiness Specification

> **Status**: Draft — forward-looking roadmap
> **Scope**: What we must do going forward to make the Streamify MVP _production-ready and scale-shaped_
> **Companion**: [`specification.md`](./specification.md) describes the target system architecture; this doc is the actionable delta between today's code and that target.

---

## 1. Context & Guiding Principle

Streamify simulates a Netflix/Spotify-scale real-time event pipeline (500K events/sec, 190+ countries, 72h late arrivals, dual-write to a real-time dashboard and a batch warehouse).

**Guiding principle:** _This is an MVP hobby project. It will not actually push 500K events/sec on a laptop. But the code must be shaped so that the only thing between us and that scale is hardware — not architectural ret‑rofit._

Concretely that means:

- No design decision that is a _ceiling below failure at scale_ (single-process write paths, global locks, unbounded memory) left unmarked.
- Configuration knobs that exist today (trigger intervals, `maxOffsetsPerTrigger`) must be _tunable toward_ the target numbers, not hard-caps that require code rewrites.
- Everything is written so a reviewer/CI can prove _"this is correct, not just working on my machine."_

### Reality check (be honest in docs and review)

| Metric            | Target        | Today (MVP config)                                        | Obvious blocker                                             |
| ----------------- | ------------- | --------------------------------------------------------- | ----------------------------------------------------------- |
| Ingestion         | 500K events/s | ~10K events/s max                                         | `maxOffsetsPerTrigger=100_000` at 10s trigger (main.py:337) |
| Dashboard latency | < 5s          | ≥ 10s                                                     | ClickHouse `processingTime` trigger = 10s (main.py:504)     |
| Late events       | 72h           | unbounded (append, no watermark)                          | no `withWatermark` — see §4.2                               |
| Write parallelism | fan-out       | single-process `insert_arrow` per batch (main.py:289-290) | collecting through the driver                               |

The plan below removes each blocker in a P0/P1/P2 order; P0s are _correctness/safety_, P1s are _scale-shaping_, P2s are _operability polish_.

---

## 2. Current-State Inventory (verified against `src/`)

### Implemented ✅

| Area                                                                | Where                                                        | Notes                                                  |
| ------------------------------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------ |
| Kafka ingestion + JSON parse + `event_id`/`event_ts`/`event_date`   | main.py:323-386, bronze_assets.py:71-126                     | `sha2(userId:sessID:ts)` event_id                      |
| Redis profile enrichment (executor-side `mapInArrow`, pipelined)    | main.py:388-400, \_enrich_profiles_partition main.py:193-253 | Arrow-native, dedup by user_id                         |
| Content metadata broadcast join                                     | main.py:402-420                                              | static CSV catalog, loaded once                        |
| ClickHouse fast-path sink (`ReplacingMergeTree`, DDL bootstrap)     | main.py:257-298, 85-126                                      | `foreachBatch` → `toArrow` → one `insert_arrow`        |
| Iceberg bronze sink (native `toTable`, partitioned by `event_date`) | main.py:464-487, bronze_assets.py:129-156                    | `fanout-enabled=true`                                  |
| Redis seeding (`seed_redis.py`)                                     | seed_redis.py                                                | async, offset commit after flush, schema registry Avro |
| Silver batch dedup + sessions assets                                | silver_assets.py                                             | `ROW_NUMBER` over `event_id`                           |
| Lightweight executor client factories                               | clients.py                                                   | `@cache`d per process                                  |

### Missing / Partial ❌

| Area                                       | State                                                                                                       |
| ------------------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| **DLQ routing**                            | only comments ("Should write to DLQ?") in seed_redis.py:119,125 — nothing implemented                       |
| **Watermarking / late-data handling**      | `withWatermark` never used anywhere in the pipeline path                                                    |
| **Schema evolution (R5)**                  | hardcoded `StructType`s in schemas.py; `schema_registry_url` used only by seed_redis.py, never the pipeline |
| **ClickHouse writes fan-out**              | single driver-side `insert_arrow` per batch — the #1 scale blocker (Req 1 + Req 4)                          |
| **Enrichment in the batch/warehouse path** | Iceberg receives raw `base_df`; only ClickHouse gets `enriched_df` (main.py:571-572)                        |
| **Backfill job (R6)**                      | possible only via `startingOffsets=earliest` + manual checkpoint reset; no first-class job                  |
| **Observability**                          | `sensors.py` exists (offset lag), but no Spark/ClickHouse write metrics, no alerting                        |
| **Idempotent ClickHouse writes**           | `batch_id` unused in `write_batch` (main.py:265); replays could double-write (mitigated only by merge tree) |

---

## 3. Non-Functional Requirements (target)

| #   | NFR                  | Target                                                                          | Prove by                                         |
| --- | -------------------- | ------------------------------------------------------------------------------- | ------------------------------------------------ |
| N1  | Throughput cap       | sustained ≥ 100K events/s single-stream on 4+ workers (scale-tunable to 500K/s) | load test + no driver-side data path             |
| N2  | Dashboard latency    | p95 event → ClickHouse-visible < 5s                                             | trigger ≤ 5s at steady state                     |
| N3  | Exactly-once / dedup | ClickHouse dedup via `(event_id)` merge key; no double-write on replay          | replay test with fixed `batch_id`                |
| N4  | Late data            | up to 72h late events land in _correct_ day's partition, never dropped silently | watermark + bounded-state test                   |
| N5  | Schema evolution     | new fields flow through w/o code deploy; never break downstream consumers       | compatibility (backward/forward) test            |
| N6  | Failure handling     | corrupt records → DLQ, never crash the stream or silently drop                  | unit test per decoder                            |
| N7  | Observability        | per-topic lag, per-batch rows, write latency visible in Dagster                 | sensor + metadata on stream assets               |
| N8  | Testability          | pipeline core decoupled from `SparkSession`/Redis/ClickHouse (injectable)       | existing tests keep passing, new pure-core tests |

---

## 4. Work Packages

Priority: **P0** must-do for correctness/safety · **P1** scale-shaping · **P2** operability.

### P0-1 — DLQ for corrupt records

- **Why:** today a malformed JSON payload crashes `from_json`'s output path or is silently swallowed; there is no failure story.
- **What:**
  - Route `from_json(...).corrupt` / schema-mismatch rows to a `dlq.events.ingestion` topic (producer-side) or an Iceberg `dlq` table.
  - Same for Redis enrichment failures (missing key is fine and defaulted; transport error → DLQ tagged with `user_id`).
  - Add `dlq.events.processing` for failed micro-batch writes.
- **Accept:** corrupt JSON lands in DLQ with `_kafka_partition/_kafka_offset`; stream stays alive; test in tests/.

### P0-2 — Watermarking + bounded event-time state

- **Why (Req 2 + N4):** events up to 72h late must land in the correct `event_date` partition. Today writes are keyed by event time already (good), but there is no watermark so nothing bounds late handling and a stateful operator (future dedup join) would never expire state.
- **What:**
  - `.withWatermark("event_ts", "72 hours")` on the enrichment/read path.
  - Keep **append** land-by-event-time semantics (no aggregation) so late rows still land in the right day partition.
  - Document the _explicit choice_: dashboard shows event-time facts late-arriving; query end-state correctness rather than "only on-time."
- **Accept:** replay a 72h-old event, verify it appears in the correct day's Iceberg partition and in ClickHouse with its original `event_ts`.

### P0-3 — Idempotent ClickHouse writes (`batch_id`)

- **Why (N3):** `foreachBatch` re-invocation on failure would re-insert rows. Merge tree dedups eventually, but "eventually" is not a contract.
- **What:**
  - Accept `batch_id` in `write_batch` and stamp rows with it (`_batch_id`, `_batch_ts`).
  - Make `ReplacingMergeTree` version column incorporate batch order so replays converge to the newest write (already keyed on `event_ts` — verify policy).
  - Unit-test the writer with a mocked client + duplicated batch.
- **Accept:** run same batch id twice → ClickHouse row count unchanged after merge.

### P1-1 — Remove the single-process write ceiling (ClickHouse)

- **Why (Req 1, Req 4, N1-N2):** `toArrow()` collects the entire micro-batch into one process, then one `insert_arrow()` writes it. That is a hard, non-scalable ceiling. This is the single highest-leverage change.
- **What — two candidate designs, pick after spike:**
  1. **Per-partition writer**: replace `foreachBatch` with a partitioned write (e.g. `foreachPartition` or a ClickHouse NativeProtocol/HTTP sink from each task) so `numPartitions` connections write concurrently. Reuses the existing executor-side `get_executor_clickhouse_client` (clients.py:23, already `@cache`d per process).
  2. **Native ClickHouse Spark connector** if the project wants to avoid hand-rolled partitioning.
  - Keep the driver closure **serialization-safe**: capture a plain params tuple, never a live `Client` (per the earlier `make_clickhouse_sink` review).
- **Accept:** micro-batch write is split across ≥ numPartitions parallel connections; no `toArrow()`/collect in the write path; throughput no longer bounded by one Python process.

### P1-2 — Scale-tunable trigger/offset configuration

- **Why (N1-N2):** today latency and throughput are coupled and hard-capped by config defaults.
- **What:**
  - Lower ClickHouse trigger to **5s** default; make `clickhouse_trigger_interval` independent from the Iceberg path.
  - Raise `maxOffsetsPerTrigger` (main.py:337) or make it per-executor-partition so it doesn't strangle ingestion; assert in config that `max_offsets ≥ expected_rate × trigger`.
  - Document the tuning formula in config comments.
- **Accept:** `docker-compose`/`.env` can express the 500K/s numbers; local MVP runs at the reduced target without code changes.

### P1-3 — Schema evolution via Schema Registry (Req 5)

- **Why:** schemas are hardcoded Python `StructType`s (schemas.py). New fields are silently dropped by `from_json`, and `create_table_if_not_exists` skips existing Iceberg tables (bronze_assets.py:56-59) so tables never gain columns. This is the _weakest claim_ in the spec today.
- **What:**
  - Move to Avro/Protobuf payloads (or JSON with explicit compatibility rules) managed by the Confluent Schema Registry; resolve the latest compatible schema per topic at source-build/parse time.
  - `from_json` with the active schema; `StructType` regenerated from the registry, not hardcoded.
  - On startup, `ALTER TABLE ... ADD COLUMN` (Iceberg schema evolution) for any new columns so downstream tables keep up.
  - Wire `schema_registry_url` (resources.py:84-87) into the _pipeline_ path (currently seed_redis-only).
  - Keep the local MVP working when no registry is reachable (fast-fall back to bundled schemas).
- **Accept:** producer adds a field; pipeline picks it up and lands it in Iceberg/ClickHouse without code change; a removed field does not crash consumers (backward-compat test).

### P1-4 — First-class backfill / replay job (Req 6)

- **Why:** replaying last 7 days today = drop checkpoint + `startingOffsets=earliest`. That's manual and re-reads _everything_.
- **What:**
  - A bounded backfill runner (Dagster `job` or a CLI flag) that sets a Kafka `startingOffsets` at `now - 7d`, uses a **fresh checkpoint**, writes into the same `ReplacingMergeTree`/Iceberg dedup targets, then stops.
  - Rely on P0-3 idempotency + merge keys so backfill **overwrites** the buggy values rather than duplicating.
  - Add a Dagster asset/sensor pattern for "replay enrichment for date range" wired to the existing silver dedup assets.
- **Accept:** simulate enrichment bug → fix → replay 7d → ClickHouse + Iceberg reflect corrected values, no duplicates.

### P2-1 — Enrichment reaches the batch warehouse (Req 3 completeness)

- **Why:** only ClickHouse gets `enriched_df` (main.py:571). Batch consumers never see the profile/content joins.
- **What:** feed the enriched frame to an Iceberg `silver`/enriched table too (separate from raw `bronze`), or persist the Redis user-dim + content-dim as proper tables so batch joins reproduce enrichment. Decide per requirement 3 semantics.

### P2-2 — Observability & monitoring

- **Why (N7):** `sensors.py` tracks Kafka lag only.
- **What:** per-stream metadata already returned in `bronze_streaming_job` (bronze_assets.py:236-247); extend to rows/batch, write latency, DLQ counts. Wire ClickHouse.write latency + Spark query progress into Dagster sensor/asset metadata; basic alert on lag > threshold.
- **Accept:** a Dagster asset shows last micro-batch latency and per-topic lag dashboards usably.

### P2-3 — Config & deploy hardening

- **What:** secrets out of `.env` (Polaris/ClickHouse creds readable only at runtime), retry/`failOnDataLoss` policy documented, healthchecks for long-running assets (see bronze_assets.py:181-185 note), and a documented `tuning.md` from config → target scale.

---

## 5. Test Strategy (must-haves)

Continue the pattern in `tests/` (unit tests with mocked Spark/Redis/ClickHouse — e.g. `conftest.py` mocks `.writeStream`, `test_bronze_assets.py`):

- `mock df + mock client` unit tests for every sink (already partially there for Iceberg; add for ClickHouse writer + DLQ).
- **Pure-core tests** for: `_enrich_profiles_partition` (arrow alignment w/ Redis stub), the clickhouse sink serialization-safety (closure captures only picklable values), decoder → DLQ routing per schema.
- **Idempotency tests**: replay same `batch_id`, verify merge-key convergence.
- **Compatibility tests**: forward/backward schema registry change simulation.
- No test should require live Kafka/ClickHouse/Redis: inject clients via the existing `@cache`/resource layer.

---

## 6. Suggested Execution Order

1. **P0-1, P0-2, P0-3** — correctness & safety (no scaling work before these).
2. **P1-4 (backfill)** — depends only on P0 idempotency; cheap and proves the merge/watermark design.
3. **P1-1 (fan-out ClickHouse write) + P1-2 (tuning)** — the scale-shaping pair; spike first.
4. **P1-3 (schema registry)** — larger; do after the write path is settled so schema changes ride on the new sink.
5. **P2-1, P2-2, P2-3** — polish, can be interleaved.

For each package: add tests + docs in the same PR; keep the `⚠️ Implementation Status` notes in README/spec accurate (today they already trail the code).
