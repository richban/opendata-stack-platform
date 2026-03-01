# Streamify Architecture - Detailed Data Flow

## System Components

```
┌─────────────────────────────────────────────────────────────────────┐
│                          LOCAL MACHINE                               │
│                                                                      │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                    DOCKER COMPOSE                              │  │
│  │                                                                │  │
│  │  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐   │  │
│  │  │  Zookeeper   │───▶│    Kafka     │    │   MinIO      │   │  │
│  │  │   :2181      │    │ :9092, :9093 │    │ :9000, :9001 │   │  │
│  │  └──────────────┘    └──────┬───────┘    └──────┬───────┘   │  │
│  │                              │                    │            │  │
│  │  ┌──────────────┐            │            ┌───────┴──────┐   │  │
│  │  │  Eventsim    │            │            │    Polaris   │   │  │
│  │  │  10 users    │────────────┘ (produces) │ REST Catalog │   │  │
│  │  │  --from 730  │                         │  :8181,:8182 │   │  │
│  │  └──────────────┘                         └──────────────┘   │  │
│  │                                                                │  │
│  │  ┌──────────────────────────────────────────────────────┐    │  │
│  │  │              SPARK CLUSTER                           │    │  │
│  │  │                                                       │    │  │
│  │  │  ┌──────────────┐         ┌──────────────┐          │    │  │
│  │  │  │ Spark Master │────────▶│ Spark Worker │          │    │  │
│  │  │  │   :8080      │         │ 2GB RAM, 2C  │          │    │  │
│  │  │  └──────────────┘         └──────────────┘          │    │  │
│  │  │                                                       │    │  │
│  │  │  ┌──────────────┐                                    │    │  │
│  │  │  │Spark Connect │  ◀── Dagster (asset execution)     │    │  │
│  │  │  │   :15002     │                                    │    │  │
│  │  │  └──────────────┘                                    │    │  │
│  │  └──────────────────────────────────────────────────────┘    │  │
│  │                                                                │  │
│  │  ┌──────────────┐                                             │  │
│  │  │   Dagster    │  Orchestrates bronze_streaming_job asset    │  │
│  │  │  (Dagster UI)│  via Spark Connect                         │  │
│  │  └──────────────┘                                             │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

## Data Flow Stages

### Stage 1: Event Generation (Eventsim → Kafka)

```
Eventsim Container
│
├─ Read: /opt/eventsim/examples/example-config.json
│         (State machine, transition probabilities)
│
├─ Generate Users (10 users, --from 730 --to 0 --continuous)
│   ├─ firstName, lastName (random from distribution)
│   ├─ gender (M/F weighted)
│   ├─ level (free: 83%, paid: 17%)
│   ├─ location (city, state, lat, lon)
│   └─ registration timestamp
│
├─ Simulate Sessions
│   ├─ Session start time (Poisson process + damping)
│   ├─ Page transitions (Markov chain)
│   │   ├─ Guest:     Register, Home, About, Help
│   │   ├─ Logged In: NextSong, Home, Settings, Upgrade, Logout
│   │   └─ Logged Out: Login, Home
│   └─ Event timing (log-normal distribution)
│
└─ Produce to Kafka (kafka:9092)
    ├─ Topic: listen_events    (NextSong pages)
    │   └─ Includes: artist, song, duration from Million Song Dataset
    ├─ Topic: page_view_events (navigation)
    │   └─ Includes: page name, method, status code
    └─ Topic: auth_events      (authentication)
        └─ Includes: success status, userId

Output: JSON events streamed continuously; 2-year history replay then real-time
```

### Stage 2: Stream Processing (Kafka → Iceberg Bronze)

```
bronze_streaming_job  (Dagster Asset — Spark Structured Streaming)
│
├─ Orchestration: Dagster (SparkConnectResource + StreamingJobConfig)
│   ├─ Connects to Spark via Spark Connect (sc://spark-master:15002)
│   ├─ Authenticates to Polaris REST Catalog via OAuth2 client credentials
│   └─ Checkpoints stored in MinIO: s3a://checkpoints/streaming/{topic}
│
├─ Read from Kafka
│   ├─ Bootstrap servers: kafka:9092
│   ├─ Subscribe: listen_events, page_view_events, auth_events
│   ├─ Starting offset: earliest
│   └─ Format: JSON strings in Kafka value field
│
├─ Parse JSON with per-topic Schemas
│   ├─ listen_events    (19 payload fields)
│   ├─ page_view_events (17 payload fields)
│   └─ auth_events      (16 payload fields)
│
├─ Transform (applied to every micro-batch)
│   ├─ Flatten JSON struct
│   ├─ Generate event_id: SHA-256(userId || '_' || sessionId || '_' || ts)
│   ├─ Extract event_date: DATE(FROM_UNIXTIME(ts / 1000))
│   ├─ Preserve Kafka metadata columns:
│   │   ├─ _kafka_partition
│   │   ├─ _kafka_offset
│   │   └─ _kafka_timestamp
│   └─ Add _processing_time: current_timestamp()
│
├─ Write to Iceberg  (micro-batch, every 30 seconds)
│   ├─ Format:       iceberg  (writeStream.format("iceberg"))
│   ├─ Output mode:  append
│   ├─ Trigger:      processingTime = "30 seconds"
│   ├─ fanout-enabled: true   (efficient partitioned writes without shuffle)
│   ├─ Checkpoint:   s3a://checkpoints/streaming/{topic}
│   ├─ Catalog:      Polaris REST Catalog
│   ├─ Warehouse:    MinIO (s3a://)
│   └─ Destination:  {catalog}.streamify.bronze_{topic}
│       Partitioned by: event_date
│
└─ Result per micro-batch
    ├─ New Iceberg snapshot committed atomically
    ├─ Data immediately queryable via Polaris / Spark SQL
    └─ Kafka offset checkpoint updated in MinIO

Throughput: ~3 Iceberg commits/minute per topic
Latency:    data visible within 30 seconds of Kafka ingestion
```

### Stage 3: Analytics (Iceberg → Spark SQL)  ⚠️ Planned

```
Spark SQL / Spark Connect
│
├─ Configure Iceberg Catalog
│   ├─ Type: REST (Polaris)
│   ├─ URI: http://polaris:8181
│   └─ Auth: OAuth2 client credentials
│
├─ Query Bronze Tables
│   ├─ {catalog}.streamify.bronze_listen_events
│   ├─ {catalog}.streamify.bronze_page_view_events
│   └─ {catalog}.streamify.bronze_auth_events
│
└─ Planned Analysis
    ├─ Top songs:        GROUP BY artist, song ORDER BY COUNT(*) DESC
    ├─ DAU/MAU:          COUNT DISTINCT userId GROUP BY event_date
    ├─ Conversion funnel: auth_events JOIN page_view_events
    ├─ Churn analysis:   WHERE auth = 'Cancelled'
    └─ Time travel:      SELECT * FROM table TIMESTAMP AS OF '...'
```

## Catalog & Storage Layout

```
Polaris REST Catalog (http://polaris:8181)
└─ Catalog: {POLARIS_CATALOG}
    └─ Namespace: streamify
        ├─ bronze_listen_events      (partitioned by event_date)
        ├─ bronze_page_view_events   (partitioned by event_date)
        └─ bronze_auth_events        (partitioned by event_date)

MinIO (s3a://)
├─ {catalog}/
│   └─ streamify/
│       ├─ bronze_listen_events/
│       │   ├─ data/event_date=2025-01-01/part-00000.parquet
│       │   └─ metadata/snap-*.avro
│       ├─ bronze_page_view_events/  (same layout)
│       └─ bronze_auth_events/       (same layout)
└─ checkpoints/
    └─ streaming/
        ├─ listen_events/        ← Spark streaming checkpoint (offsets + state)
        ├─ page_view_events/
        └─ auth_events/
```

## Data Schemas

### Bronze: listen_events

```
# Payload fields (from Eventsim via Kafka)
artist:          string
song:            string
duration:        double   (seconds)
ts:              long     (milliseconds epoch)
auth:            string   (Guest | Logged In | Logged Out | Cancelled)
level:           string   (free | paid)
city:            string
zip:             string
state:           string
userAgent:       string
lon:             double
lat:             double
userId:          long
lastName:        string
firstName:       string
gender:          string   (M | F)
registration:    long     (milliseconds epoch)
sessionId:       int
itemInSession:   int

# Added by Spark streaming transform
event_id:           string     SHA-256(userId_sessionId_ts)
event_date:         date       Derived from ts — Iceberg partition key
_kafka_partition:   int
_kafka_offset:      long
_kafka_timestamp:   timestamp
_processing_time:   timestamp
```

### Bronze: page_view_events

```
# Payload fields
ts, sessionId, auth, level, itemInSession,
city, zip, state, userAgent, lon, lat,
userId, lastName, firstName, gender, registration,
page:   string   (Home | About | Settings | Help | Upgrade | Downgrade | Error | …)

# Added by transform (same as listen_events)
event_id, event_date, _kafka_partition, _kafka_offset,
_kafka_timestamp, _processing_time
```

### Bronze: auth_events

```
# Payload fields
ts, sessionId, level, itemInSession,
city, zip, state, userAgent, lon, lat,
userId, lastName, firstName, gender, registration,
success: string   (true | false)

# Added by transform (same as listen_events)
event_id, event_date, _kafka_partition, _kafka_offset,
_kafka_timestamp, _processing_time
```

## Resource Allocation

```
Component            | Memory | CPU | Storage
---------------------|--------|-----|---------------------------
Eventsim             | 6GB    | 1   | <1GB (JAR + data files)
Kafka + Zookeeper    | 2GB    | 1   | grows with log retention
Polaris              | 1GB    | 1   | minimal (metadata only)
Spark Master         | 1GB    | 1   | minimal
Spark Worker         | 2GB    | 2   | minimal
Spark Connect        | 2GB    | 1   | minimal
MinIO (data)         | -      | -   | ~5–10 MB per 30s batch
MinIO (checkpoints)  | -      | -   | <100 MB total
---------------------|--------|-----|---------------------------
Total Recommended    | 10GB+  | 5+  | 10GB+ for long runs
```

## Monitoring Points

1. **Event Generation Rate**
   - Check: `docker logs eventsim`
   - Expected: continuous JSON events to Kafka topics

2. **Kafka Topic Lag**
   - Check: Kafdrop UI (http://localhost:9002)
   - Expected: consumer lag < 1000 messages per topic

3. **Streaming Job Health**
   - Check: Spark UI (http://localhost:8080) → active streaming application
   - Check: Dagster UI → `bronze_streaming_job` asset run logs
   - Expected: micro-batches completing in < 30 seconds each

4. **Iceberg Commit Rate**
   - Check: Polaris Console (http://localhost:3001)
   - Expected: new snapshots every ~30 seconds per table

5. **Checkpoint Progress**
   - Check: MinIO Console (http://localhost:9001) → `checkpoints/` bucket
   - Expected: offset files updating continuously per topic

6. **MinIO Storage Growth**
   - Check: MinIO Console → bucket sizes
   - Expected: steady linear growth proportional to event rate

## Failure Scenarios & Recovery

1. **Eventsim Crash**
   - Recovery: `docker compose restart eventsim`
   - Impact: gap in Kafka topics; historical replay resumes from `--from 730` on restart
   - Duration: ~10 seconds

2. **Kafka Unavailable**
   - Recovery: `docker compose restart zookeeper kafka`
   - Impact: Spark streaming will retry; resumes from last checkpointed Kafka offset
   - Duration: ~30 seconds

3. **Spark Streaming Job Fails**
   - Recovery: re-materialise `bronze_streaming_job` in the Dagster UI
   - Impact: **no data loss** — Spark resumes from the last committed offset in the checkpoint
   - Duration: picks up exactly where it left off

4. **Polaris Unavailable**
   - Recovery: `docker compose restart polaris`
   - Impact: Spark cannot commit Iceberg snapshots; streaming job will fail
   - Recovery path: Kafka retains all messages — restart streaming job after Polaris is healthy

5. **MinIO Unavailable**
   - Recovery: `docker compose restart minio`
   - Impact: both data writes and checkpoint updates fail; streaming job stops
   - Recovery path: no data loss from Kafka — restart `bronze_streaming_job` once MinIO is healthy

6. **Disk Full**
   - Recovery: clean old Iceberg snapshots or expand storage
   - Prevention: monitor MinIO bucket growth; schedule `expire_snapshots`

## Performance Tuning

### Increase Throughput
- More Eventsim users: change `--nusers` in `docker-compose.yml`
- Add Spark workers: scale out the cluster
- Increase Kafka partitions per topic (default: 1)
- `fanout-enabled: true` is already set for efficient partitioned Iceberg writes

### Reduce Streaming Latency
- Lower the trigger interval: `processingTime="10 seconds"` (more commits, smaller files)
- More Kafka partitions → more Spark tasks → higher parallelism

### Reduce Resource Usage
- Fewer Eventsim users: `--nusers 10` (current default)
- Longer trigger interval: `processingTime="120 seconds"` (fewer, larger commits)
- Lower Spark worker memory: `SPARK_WORKER_MEMORY=1G`

### Iceberg File Management ⚠️ Planned
- Compact small files periodically:
  ```sql
  CALL {catalog}.system.rewrite_data_files(table => 'streamify.bronze_listen_events')
  ```
- Expire old snapshots to control metadata growth:
  ```sql
  CALL {catalog}.system.expire_snapshots(table => 'streamify.bronze_listen_events',
                                          older_than => TIMESTAMP '2025-01-01 00:00:00')
  ```

---

**Diagram Version**: 2.0
**Last Updated**: March 2026
**Architecture**: Kafka → Spark Structured Streaming (micro-batch, 30s) → Iceberg (Polaris REST Catalog + MinIO)
