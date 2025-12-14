# Team Ops - Streamify Data Platform

Real-time music streaming analytics pipeline using Kafka, Spark, Iceberg, and Dagster. Implements a Lambda architecture with streaming (Dagster Pipes) and batch (Spark Connect) layers.

> **📖 Quick Start**: See [`/QUICKSTART.md`](../../QUICKSTART.md) for immediate setup  
> **📚 Full Guide**: See [`/SETUP.md`](../../SETUP.md) for detailed documentation

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                   STREAMING LAYER (24/7)                        │
│                   Orchestration: Dagster Pipes                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Kafka Topics  ──spark-submit──>  Spark Streaming  ──────>     │
│  ├─ listen_events              (Pipes monitors via S3)         │
│  ├─ page_view_events                    │                      │
│  └─ auth_events                          ▼                      │
│                              Iceberg Bronze Tables              │
│                              (Polaris Catalog)                  │
│                              ├─ bronze_listen_events            │
│                              ├─ bronze_page_view_events         │
│                              └─ bronze_auth_events              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                     BATCH LAYER (Hourly)                        │
│                   Orchestration: Spark Connect                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Dagster Assets (Direct PySpark API)                           │
│                                                                 │
│  Bronze Tables ──dedup──> Silver Tables                        │
│                           ├─ silver_listen_events               │
│                           ├─ silver_page_view_events            │
│                           ├─ silver_auth_events                 │
│                           └─ silver_user_sessions (sessionized) │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Two-Layer Approach

### 1. Streaming Layer (Dagster Pipes)

**Purpose**: Real-time ingestion from Kafka to Bronze Iceberg tables

**Asset**: `bronze_streaming_job`

**Technology**:
- **Orchestration**: Dagster Pipes
- **Execution**: `spark-submit` subprocess  
- **Monitoring**: S3-based message passing (logs, metrics)
- **Script**: `spark_scripts/stream_to_iceberg.py`

**Why Dagster Pipes?**
- Manages long-running streaming jobs (24/7)
- Non-blocking: Dagster doesn't wait for infinite stream
- Observable: Logs/metrics flow back via S3
- Restartable: Can programmatically kill/restart jobs

**Features**:
- Continuous ingestion with Spark checkpointing
- Auto-generated deduplication hash (`event_id`)
- Date partitioning (`event_date`)
- Metadata enrichment (`_processing_time`, `_kafka_partition`, `_kafka_offset`)

### 2. Batch Layer (Spark Connect)

**Purpose**: Scheduled transformations (dedup, sessionization, quality checks)

**Assets**: `silver_listen_events`, `silver_page_view_events`, etc.

**Technology**:
- **Orchestration**: Dagster assets
- **Execution**: Spark Connect (direct PySpark API via gRPC)
- **Resource**: `SparkConnectResource`
- **Location**: `defs/assets.py`

**Why Spark Connect?**
- Native PySpark API from Dagster (no subprocess overhead)
- Better error handling (stack traces preserved)
- Resource injection via Dagster
- Simpler code (no CLI argument passing)

**Features**:
- Hourly deduplication using window functions
- Session reconstruction (30-min timeout)
- Iceberg MERGE/DELETE operations
- Data quality checks

## Data Flow

### Event Types

1. **Listen Events**: Song plays (artist, song, duration, user, session)
2. **Page View Events**: Navigation (page, user, timestamp)
3. **Auth Events**: Authentication (login, logout, success status)

### Bronze Layer (Streaming)

```sql
-- Partitioned by event_date, includes dedup hash
CREATE TABLE bronze_listen_events (
    -- Original Kafka event fields
    artist STRING,
    song STRING,
    duration DOUBLE,
    ts BIGINT,
    userId BIGINT,
    sessionId INT,
    -- ... (other fields)
    
    -- Added by streaming job
    event_id STRING,           -- SHA256(userId|sessionId|ts)
    event_date DATE,           -- PARTITION KEY
    _processing_time TIMESTAMP,
    _kafka_partition INT,
    _kafka_offset BIGINT
)
USING iceberg
PARTITIONED BY (event_date)
```

### Silver Layer (Batch)

```sql
-- Deduplicated using event_id
CREATE TABLE silver_listen_events
USING iceberg
PARTITIONED BY (event_date)
AS SELECT ... FROM bronze_listen_events

-- Sessionized events (30-min timeout)
CREATE TABLE silver_user_sessions (
    user_id BIGINT,
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    event_count INT,
    ...
)
```

## Setup

### 1. Start Infrastructure

```bash
docker-compose up -d
```

**Services**:
- Kafka + Zookeeper (9092, 9093)
- Spark Master + Workers (8080)
- Eventsim (event generator)
- MinIO (S3-compatible storage)
- Polaris (Iceberg REST catalog)

### 2. Bootstrap Polaris Catalog

```bash
./polaris-config/setup_polaris.sh
```

Outputs credentials to `polaris-config/polaris_credentials.env`:
```bash
export POLARIS_CLIENT_ID="principal_xxxxx"
export POLARIS_CLIENT_SECRET="secret_xxxxx"
```

### 3. Set Environment Variables

```bash
# Spark Connect (batch layer)
export SPARK_REMOTE="sc://spark-master:15002"

# Polaris credentials
export POLARIS_CLIENT_ID="..."
export POLARIS_CLIENT_SECRET="..."

# MinIO (S3 for Pipes)
export AWS_ACCESS_KEY_ID="minio"
export AWS_SECRET_ACCESS_KEY="minio123"
export AWS_ENDPOINT_URL="http://minio:9000"
export DAGSTER_PIPES_BUCKET="dagster-pipes"
```

### 4. Install Dependencies

```bash
cd dagster-workspace/team_ops
uv sync
source .venv/bin/activate
```

## Usage

### Launch Streaming Job (Dagster Pipes)

```bash
cd dagster-workspace
dagster asset materialize -m team_ops.definitions -a bronze_streaming_job
```

**What happens**:
1. Dagster uploads `stream_to_iceberg.py` to S3
2. Launches `spark-submit` with Pipes bootstrap params
3. Spark job connects to Kafka, starts streaming
4. Logs/metrics flow back to Dagster via S3 messages
5. Dagster UI shows real-time progress

**Check logs**:
```bash
# Dagster UI: http://localhost:3000
# Spark UI: http://localhost:8080
```

### Run Batch Deduplication (Spark Connect)

```bash
# Deduplicate yesterday's listen events
dagster asset materialize -m team_ops.definitions -a silver_listen_events
```

**What happens**:
1. Dagster creates SparkSession via Spark Connect (gRPC)
2. Runs PySpark code directly (no subprocess)
3. Reads from Bronze, deduplicates by `event_id`
4. Writes to Silver with Iceberg MERGE operation

### Monitor Kafka Events

```bash
# List topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Consume listen_events
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic listen_events \
  --max-messages 10
```

## Project Structure

```
team_ops/
├── src/team_ops/
│   ├── spark_scripts/
│   │   └── stream_to_iceberg.py    # Pipes streaming script
│   ├── defs/
│   │   ├── streaming_assets.py     # Pipes orchestration
│   │   ├── assets.py               # Spark Connect batch jobs
│   │   ├── resources.py            # SparkConnectResource
│   │   └── definitions.py          # Dagster definitions
│   └── definitions.py              # Entry point
├── pyproject.toml
└── README.md
```

## Key Design Decisions

### Why Two Approaches?

| Layer | Technology | Reason |
|-------|-----------|--------|
| **Streaming** | Dagster Pipes | Long-running jobs (24/7), subprocess monitoring |
| **Batch** | Spark Connect | Scheduled jobs, native PySpark API, better errors |

### Dagster Pipes (Streaming)

**Problem**: Spark Structured Streaming runs indefinitely, but Dagster assets expect bounded execution.

**Solution**: Pipes launches `spark-submit` as subprocess and monitors via S3:
- Dagster doesn't block waiting for stream to end
- Logs/metrics streamed back via S3 messages
- Can programmatically restart/kill jobs
- Works with any Spark cluster (local, EMR, Databricks)

### Spark Connect (Batch)

**Problem**: Batch transformations need direct PySpark API for Dagster semantics (dependencies, partitions).

**Solution**: Spark Connect provides native PySpark over gRPC:
- No subprocess overhead
- Direct DataFrame operations
- Dagster manages SparkSession lifecycle
- Better error handling (full stack traces)

## Dependencies

```toml
dagster==1.12.3           # Orchestration framework
dagster-aws==0.28.3       # S3 integration for Pipes
dagster-pipes>=1.12.3     # Pipes protocol
pyspark==3.5.3            # Spark Connect + streaming scripts
boto3>=1.35.0             # S3 client
grpcio>=1.48.1            # Spark Connect gRPC
grpcio-status>=1.48.1     # gRPC status codes
pyiceberg[pyarrow]==0.8.1 # Iceberg Python API
pandas>=1.0.5             # Required by Spark Connect
pyarrow>=10.0.0           # Arrow format
```

## Catalog Structure (Polaris)

```
lakehouse                    # Catalog
└── streamify                # Namespace
    ├── bronze_listen_events      # Streaming writes
    ├── bronze_page_view_events
    ├── bronze_auth_events
    ├── silver_listen_events      # Batch dedup
    ├── silver_page_view_events
    ├── silver_auth_events
    └── silver_user_sessions      # Sessionization
```

## Storage (MinIO)

```
lakehouse/           # Iceberg table data
checkpoints/         # Spark streaming checkpoints
dagster-pipes/       # Pipes message passing
```

## Monitoring

### Dagster UI (http://localhost:3000)
- Asset lineage graph
- Materialization history
- Logs and metadata
- Run timeline

### Spark UI (http://localhost:8080)
- Active/completed jobs
- Stage execution details
- Executor metrics

### Kafka Monitoring
```bash
# Consumer lag
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group streaming_job
```

## Troubleshooting

### Streaming job won't start

```bash
# Check Kafka
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Check Spark master
docker ps | grep spark-master

# Check Polaris
curl http://localhost:8181/healthcheck
```

### Polaris authentication fails

```bash
# Regenerate credentials
./polaris-config/setup_polaris.sh

# Verify credentials
echo $POLARIS_CLIENT_ID
echo $POLARIS_CLIENT_SECRET
```

### Spark Connect timeout

```bash
# Check Spark Connect port (15002)
docker exec spark-master netstat -tuln | grep 15002

# Test connection
telnet spark-master 15002
```

### S3 errors (Pipes communication)

```bash
# Check MinIO
docker exec minio mc admin info local

# List buckets
docker exec minio mc ls local/
```

# Architecture Flow
Eventsim (generates events)
    ↓
Kafka (kafka:9092) - Event streaming
    ↓
Dagster (Mac:3000) - Orchestration
    ↓ gRPC (Spark Connect Protocol)
Spark Connect (Docker:15002)
    ↓ submits to
Spark Master/Workers (Docker)
    ↓ writes Iceberg tables via
Apache Polaris (polaris:8181) - REST catalog
    ↓ stores metadata
    ↓ stores data files
MinIO (minio:9000) - S3-compatible storage
Polaris Console (localhost:3001) - Web UI
    ↓ HTTP API
Apache Polaris (localhost:8181)
All services communicate on: opendata_network (single Docker bridge network)

## Future Work

### Streaming Layer
- [ ] Add Dagster sensors to detect Bronze updates
- [ ] Implement health checks for streaming jobs
- [ ] Add Kafka lag monitoring
- [ ] Create alerts for stream failures

### Batch Layer
- [ ] Complete `silver_page_view_events` deduplication
- [ ] Complete `silver_auth_events` deduplication
- [ ] Implement session reconstruction (`silver_user_sessions`)
- [ ] Add data quality checks (Great Expectations)
- [ ] Implement Iceberg compaction/maintenance
- [ ] Add time-based partitioning

### Gold Layer
- [ ] Migrate to SQLMesh for dbt-style SQL transformations
- [ ] Add customer churn prediction models
- [ ] Add user path analysis
- [ ] Add real-time feature serving

## References

- [Dagster Pipes Documentation](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes)
- [PySpark + Pipes Guide](https://docs.dagster.io/guides/build/external-pipelines/pyspark-pipeline)
- [Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html)
- [Apache Iceberg](https://iceberg.apache.org/)
- [Polaris Catalog](https://github.com/apache/polaris)
