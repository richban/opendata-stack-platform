# Team Ops - Streamify Data Platform

Real-time music streaming analytics pipeline using Kafka, Spark, Iceberg, Polaris Catalog, and Dagster. Ingests simulated Spotify-like events from Eventsim into Iceberg Bronze tables via Spark Structured Streaming, orchestrated through Dagster via Spark Connect.

> **⚠️ Implementation Status**: The Bronze streaming layer is implemented but not verified yet.
> The Silver/Gold batch layers are planned but not yet implemented.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                   STREAMING LAYER                               │
│                   Orchestration: Dagster → Spark Connect        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Eventsim ──▶ Kafka Topics ──▶ Spark Structured Streaming ──▶   │
│               ├─ listen_events     (via Spark Connect gRPC)     │
│               ├─ page_view_events  30-second micro-batches      │
│               └─ auth_events                │                   │
│                                             ▼                   │
│                                 Iceberg Bronze Tables           │
│                                 (Polaris REST Catalog)          │
│                                 ├─ bronze_listen_events         │
│                                 ├─ bronze_page_view_events      │
│                                 └─ bronze_auth_events           │
│                                             │                   │
│                                             ▼                   │
│                                 MinIO (s3a://)                  │
│                                 ├─ Table data files             │
│                                 └─ Streaming checkpoints        │
└─────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                  BATCH LAYER  ⚠️ Planned (not implemented)      │
│                  Orchestration: Dagster assets + Spark Connect  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Bronze Tables ──dedup──▶ Silver Tables                        │
│                            ├─ silver_listen_events              │
│                            ├─ silver_page_view_events           │
│                            ├─ silver_auth_events                │
│                            └─ silver_user_sessions (sessionized)│
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## How It Actually Works

### Streaming Layer (Implemented ✅)

The **only implemented asset** is `bronze_streaming_job`. It uses **Spark Connect** (not Dagster Pipes) to run Spark Structured Streaming directly from Dagster:

```
Dagster Asset (bronze_streaming_job)
   │
   ├─ Creates SparkSession via Spark Connect (sc://spark-master:15002)
   │   └─ SparkConnectResource handles OAuth2 auth to Polaris
   │
   ├─ For each Kafka topic (listen_events, page_view_events, auth_events):
   │   ├─ Creates Iceberg table in Polaris if not exists
   │   ├─ Reads Kafka stream (startingOffsets: earliest)
   │   ├─ Parses JSON with schema from schemas.py
   │   ├─ Transforms:
   │   │   ├─ event_id = SHA-256(userId_sessionId_ts)
   │   │   ├─ event_date = DATE(FROM_UNIXTIME(ts / 1000))
   │   │   └─ Preserves _kafka_partition, _kafka_offset, _kafka_timestamp
   │   └─ Writes to Iceberg:
   │       ├─ format("iceberg").toTable("{catalog}.streamify.bronze_{topic}")
   │       ├─ trigger(processingTime="30 seconds")
   │       ├─ outputMode("append")
   │       └─ fanout-enabled: true
   │
   └─ Calls session.streams.awaitAnyTermination()
       (runs indefinitely until stopped or crashes)
```

> **Important**: `bronze_streaming_job` is a **long-running asset**. Once materialised it blocks
> the Dagster run indefinitely. It must be cancelled explicitly via the Dagster UI to stop streaming.

### Batch Layer (Planned ⚠️)

No silver or gold assets are implemented yet. The `SparkConnectResource` is already wired up and ready for batch assets to use. See [Future Work](#future-work).

---

## Project Structure

```
streamify/
├── src/streamify/
│   ├── schemas.py                  # PySpark schemas for all 3 Kafka topics + meta_schema
│   ├── definitions.py              # Entry point → streamify.defs.definitions
│   └── defs/
│       ├── __init__.py             # Package docstring
│       ├── assets.py               # bronze_streaming_job asset (only asset)
│       ├── resources.py            # SparkConnectResource, StreamingJobConfig, S3Resource
│       └── definitions.py          # Dagster Definitions (resources wired up)
├── notebooks/
│   ├── test_streaming_simple.py    # Standalone test: Kafka → parse → console output
│   └── test_streaming.py           # Extended streaming test
├── pyproject.toml
└── README.md
```

**What does NOT exist (yet)**:
- `spark_scripts/` directory — no external `spark-submit` scripts
- `defs/streaming_assets.py` — streaming is in `assets.py`
- Silver layer assets (`silver_listen_events`, etc.)
- Gold layer assets

---

## Data Schemas

Schemas are defined in `schemas.py` and imported by `assets.py`.

### listen_events (19 payload fields)
```
artist, song, duration (double), ts (long ms), auth, level,
city, zip, state, userAgent, lon, lat (double),
userId (long), lastName, firstName, gender,
registration (long ms), sessionId (int), itemInSession (int)
```

### page_view_events (17 payload fields)
```
ts (long ms), sessionId (int), auth, level, itemInSession (int),
city, zip, state, userAgent, lon, lat (double),
userId (long), lastName, firstName, gender, registration (long ms),
page  (Home | About | Settings | Help | Upgrade | Downgrade | Error | …)
```

### auth_events (16 payload fields)
```
ts (long ms), sessionId (int), level, itemInSession (int),
city, zip, state, userAgent, lon, lat (double),
userId (long), lastName, firstName, gender, registration (long ms),
success (string: "true" | "false")
```

### meta_schema (added by streaming transform to all topics)
```
event_id          string     SHA-256(userId_sessionId_ts)
event_date        date       Partition key — derived from ts
_kafka_partition  int
_kafka_offset     long
_kafka_timestamp  timestamp
_processing_time  timestamp
```

---

## Catalog Structure (Polaris)

```
{POLARIS_CATALOG}            ← catalog name from env
└── streamify                ← namespace (POLARIS_NAMESPACE)
    ├── bronze_listen_events      ✅ created by bronze_streaming_job
    ├── bronze_page_view_events   ✅ created by bronze_streaming_job
    ├── bronze_auth_events        ✅ created by bronze_streaming_job
    ├── silver_listen_events      ⚠️ planned
    ├── silver_page_view_events   ⚠️ planned
    ├── silver_auth_events        ⚠️ planned
    └── silver_user_sessions      ⚠️ planned
```

## Storage (MinIO)

```
{catalog}/           ← Iceberg table data (Parquet files)
checkpoints/         ← Spark streaming checkpoints (s3a://checkpoints/streaming/{topic})
dagster-pipes/       ← Reserved bucket (currently unused)
```

---

## Setup

### 1. Start Infrastructure

```bash
docker compose up -d
```

**Services started**:
- Eventsim — event generator (10 simulated users, 2-year backfill + continuous)
- Kafka + Zookeeper — `9092` (internal), `9093` (host)
- Spark Master + Worker + Connect — `8080` (UI), `15002` (Connect gRPC)
- MinIO — `9000` (API), `9001` (console)
- Polaris REST Catalog — `8181` (API), `8182` (health)
- Kafdrop — `9002` (Kafka UI)

### 2. Bootstrap Polaris Catalog

```bash
# Run once — creates catalog, namespace, principal, and credentials
docker compose up polaris-init
```

Credentials are written to `polaris-config/polaris_credentials.env`.

### 3. Set Environment Variables

```bash
# Copy the generated credentials
source polaris-config/polaris_credentials.env

# Spark Connect
export SPARK_REMOTE="sc://localhost:15002"

# MinIO
export AWS_ACCESS_KEY_ID="miniouser"
export AWS_SECRET_ACCESS_KEY="miniouser"
export AWS_ENDPOINT_URL="http://localhost:9000"

# Polaris
export POLARIS_URI="http://localhost:8181/api/catalog"
export POLARIS_CATALOG="lakehouse"
export POLARIS_NAMESPACE="streamify"

# Dagster Pipes (reserved — currently unused)
export DAGSTER_PIPES_BUCKET="dagster-pipes"

# Checkpoint path
export CHECKPOINT_PATH="s3a://checkpoints/streaming"

# Kafka (Dagster runs on host, Spark runs in Docker)
export KAFKA_BOOTSTRAP_SERVERS="localhost:9093"
```

### 4. Install Dependencies

```bash
cd dagster-workspace/streamify
uv sync
source .venv/bin/activate
```

### 5. Start Dagster

```bash
cd dagster-workspace
dagster dev -m streamify.definitions
```

---

## Usage

### Launch the Bronze Streaming Job

```bash
dagster asset materialize -m streamify.definitions -a bronze_streaming_job
```

Or materialise via the Dagster UI at `http://localhost:3000`.

**What actually happens**:
1. Dagster connects to Spark via Spark Connect (gRPC to `sc://spark-master:15002`)
2. Polaris namespace `streamify` is created (idempotent)
3. For each topic, an Iceberg table `bronze_{topic}` is created if it doesn't exist
4. Three streaming queries start (one per topic), writing 30-second micro-batches to Iceberg
5. The asset run stays **active indefinitely** — cancel it from the Dagster UI to stop streaming

**Check progress**:
- Dagster UI: `http://localhost:3000` — run logs and materialisation events
- Spark UI: `http://localhost:8080` — active streaming jobs and batch statistics
- Polaris Console: `http://localhost:3001` — verify tables and snapshots exist
- Kafdrop: `http://localhost:9002` — inspect Kafka topics and consumer lag
- MinIO Console: `http://localhost:9001` — verify data files in `{catalog}/streamify/`

### Test the Pipeline Independently (Without Dagster)

```bash
cd dagster-workspace/streamify
source .venv/bin/activate

# Runs: Kafka read → JSON parse → transform → console output (10 seconds)
python notebooks/test_streaming_simple.py
```

### Monitor Kafka Topics

```bash
# List topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Inspect consumer lag
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --all-groups

# Consume sample events
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic listen_events \
  --max-messages 5
```

---

## Key Design Decisions

### Why Spark Connect (not Dagster Pipes) for Streaming?

| Approach | Status | Rationale |
|----------|--------|-----------|
| **Spark Connect** (current) | ✅ In use | Direct PySpark API via gRPC; Dagster manages session lifecycle; native error handling |
| **Dagster Pipes + spark-submit** | ❌ Not used | Would require external script, S3 message-passing; more moving parts for same outcome |

Spark Connect allows the streaming job to be written as native PySpark code inside a Dagster asset, with zero subprocess overhead. The tradeoff is that the asset run blocks indefinitely — which is acceptable for a long-running streaming job managed via the Dagster UI.

### Why Iceberg Directly (not Parquet staging)?

Writing directly to Iceberg from Spark Structured Streaming gives:
- **ACID commits** every 30 seconds — data immediately queryable
- **No duplicate storage** — no intermediate Parquet files to manage
- **Schema evolution** — `ALTER TABLE` works without rewriting the pipeline
- **Checkpointing** into MinIO ensures exactly-once semantics on restart

### fanout-enabled: true

Set on the `writeStream` writer. Required for efficient Iceberg writes when partitioning is enabled — allows the writer to open multiple partition files concurrently without a pre-shuffle.

---

## Dependencies

From `pyproject.toml`:

```toml
dagster==1.12.3
dagster-aws==0.28.3         # S3Resource (MinIO)
dagster-pipes>=1.12.3       # Available but not currently used
pyspark==3.5.3              # Spark Connect client + streaming
kafka-python==2.0.2
pyiceberg[pyarrow]==0.8.1   # Iceberg Python API (available for future use)
pandas>=1.0.5               # Required by Spark Connect
pyarrow>=10.0.0
grpcio>=1.48.1              # Spark Connect transport
grpcio-status>=1.48.1
googleapis-common-protos>=1.56.4
```

---

## Troubleshooting

### Streaming job won't start

```bash
# Is Kafka healthy?
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Is Spark Connect listening?
docker exec spark-connect netstat -tuln | grep 15002

# Is Polaris healthy?
curl http://localhost:8182/q/health
```

### Polaris authentication fails

```bash
# Re-run the init container to regenerate credentials
docker compose run --rm polaris-init

# Check the generated credentials
cat polaris-config/polaris_credentials.env
```

### Spark Connect timeout / connection refused

```bash
# Check that the spark-connect container is running
docker ps | grep spark-connect

# The Connect server takes ~60s to start (downloads Spark packages)
# Watch its logs
docker logs spark-connect -f
```

### Streaming stops after restart

Spark recovers automatically from the MinIO checkpoint. Just re-materialise
`bronze_streaming_job` — it will resume from the last committed Kafka offset.

### MinIO / S3 errors

```bash
# Check MinIO is healthy
curl http://localhost:9000/minio/health/live

# List buckets via mc
docker exec minio mc ls local/
```

---

## Future Work

### Streaming Layer
- [ ] Dagster sensor to detect streaming job health and auto-restart on failure
- [ ] Kafka consumer lag monitoring / alerting
- [ ] Periodic Iceberg compaction (`rewrite_data_files`) as a scheduled Dagster asset

### Batch Layer (Silver)
- [ ] `silver_listen_events` — deduplicate bronze by `event_id`
- [ ] `silver_page_view_events` — deduplicate bronze
- [ ] `silver_auth_events` — deduplicate bronze
- [ ] `silver_user_sessions` — session reconstruction (30-min timeout)
- [ ] Data quality checks (e.g. null `userId`, duplicate `event_id` ratio)

### Gold Layer
- [ ] Top tracks / artists aggregations
- [ ] DAU / MAU metrics
- [ ] User churn analysis (`auth = 'Cancelled'`)
- [ ] User conversion funnel (Guest → Registered → Paid)
- [ ] Potential SQLMesh integration for SQL-first transformations

---

## References

- [Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html)
- [Spark Structured Streaming + Iceberg](https://iceberg.apache.org/docs/latest/spark-writes/#streaming-writes)
- [Apache Iceberg](https://iceberg.apache.org/)
- [Apache Polaris Catalog](https://github.com/apache/polaris)
- [Dagster Assets](https://docs.dagster.io/guides/build/assets/)
