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
│  │  └──────────────┘    └──────┬───────┘    └──────────────┘   │  │
│  │                              │                                │  │
│  │  ┌──────────────┐            │                                │  │
│  │  │  Eventsim    │            │                                │  │
│  │  │  10K users   │────────────┘ (produces)                    │  │
│  │  │              │                                             │  │
│  │  │ listen_events, page_view_events, auth_events              │  │
│  │  └──────────────┘                                             │  │
│  │                                                                │  │
│  │  ┌──────────────────────────────────────────────────────┐    │  │
│  │  │              SPARK CLUSTER                           │    │  │
│  │  │                                                       │    │  │
│  │  │  ┌──────────────┐         ┌──────────────┐          │    │  │
│  │  │  │ Spark Master │────────▶│ Spark Worker │          │    │  │
│  │  │  │   :8080      │         │ 2GB RAM, 2C  │          │    │  │
│  │  │  └──────────────┘         └──────────────┘          │    │  │
│  │  │                                                       │    │  │
│  │  │  Volumes:                                            │    │  │
│  │  │  - /data (shared storage)                            │    │  │
│  │  │  - /opt/team_ops (pipeline code)                     │    │  │
│  │  └──────────────────────────────────────────────────────┘    │  │
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
├─ Generate Users: 10,000 users with attributes
│   ├─ firstName, lastName (random from distribution)
│   ├─ gender (M/F weighted)
│   ├─ level (free: 83%, paid: 17%)
│   ├─ location (city, state, lat, lon)
│   └─ registration timestamp
│
├─ Simulate Sessions
│   ├─ Session start time (Poisson process + damping)
│   ├─ Page transitions (Markov chain)
│   │   ├─ Guest: Register, Home, About, Help
│   │   ├─ Logged In: NextSong, Home, Settings, Upgrade, Logout
│   │   └─ Logged Out: Login, Home
│   └─ Event timing (log-normal distribution)
│
└─ Produce to Kafka
    ├─ Topic: listen_events (NextSong pages)
    │   └─ Includes: artist, song, duration from Million Song Dataset
    ├─ Topic: page_view_events (navigation)
    │   └─ Includes: page name, method, status code
    └─ Topic: auth_events (authentication)
        └─ Includes: success status, userId

Output: ~100-200 JSON events/second to Kafka
```

### Stage 2: Stream Processing (Kafka → Data Lake)

```
stream_music_events.py (Spark Streaming)
│
├─ Read from Kafka
│   ├─ Bootstrap servers: kafka:9092
│   ├─ Subscribe: listen_events, page_view_events, auth_events
│   ├─ Starting offset: earliest
│   └─ Format: JSON strings in Kafka value field
│
├─ Parse JSON with Schemas
│   ├─ Listen Events Schema (19 fields)
│   │   ├─ artist: StringType
│   │   ├─ song: StringType
│   │   ├─ ts: LongType (milliseconds)
│   │   ├─ userId: LongType
│   │   └─ ...
│   ├─ Page View Events Schema (17 fields)
│   └─ Auth Events Schema (16 fields)
│
├─ Transform
│   ├─ Convert ts (ms) → ts_timestamp (timestamp)
│   ├─ Extract: year, month, day, hour
│   └─ Add metadata columns
│
├─ Write to Parquet (every 2 minutes)
│   ├─ Format: Parquet with Snappy compression
│   ├─ Partition by: month, day, hour
│   ├─ Location: /data/lake/{topic}/month={m}/day={d}/hour={h}/
│   ├─ Checkpoint: /data/checkpoints/{topic}/
│   └─ Mode: append
│
└─ Output Structure
    /data/lake/
    ├─ listen_events/
    │   └─ month=12/day=2/hour=15/
    │       ├─ part-00000-uuid.snappy.parquet
    │       └─ part-00001-uuid.snappy.parquet
    ├─ page_view_events/...
    └─ auth_events/...

Output: 10K-30K records per 2-minute batch
Size: ~5-10MB per batch (compressed)
```

### Stage 3: Batch Loading (Data Lake → Iceberg)

```
hourly_batch_load.py (Spark Batch)
│
├─ Input Parameters
│   ├─ --year 2024
│   ├─ --month 12
│   ├─ --day 2
│   └─ --hour 15
│
├─ Read Parquet from Data Lake
│   └─ Path: /data/lake/{topic}/month={m}/day={d}/hour={h}/*.parquet
│
├─ Add Metadata
│   └─ load_timestamp: current datetime
│
├─ Create/Update Iceberg Tables
│   ├─ Catalog: local (Hadoop catalog)
│   ├─ Warehouse: /data/warehouse
│   ├─ Database: streamify
│   ├─ Tables:
│   │   ├─ listen_events_staging
│   │   ├─ page_view_events_staging
│   │   └─ auth_events_staging
│   └─ Format: Iceberg with Parquet data files
│
└─ Append Data
    ├─ Mode: append (not overwrite)
    ├─ ACID guarantees via Iceberg
    ├─ Snapshot isolation
    └─ Time travel enabled

Output: Iceberg tables at /data/warehouse/streamify/
Metadata: /data/warehouse/streamify/{table}/metadata/
Data: /data/warehouse/streamify/{table}/data/
```

### Stage 4: Analytics (Iceberg → Spark SQL)

```
Spark SQL / spark-shell
│
├─ Configure Iceberg Catalog
│   ├─ Extensions: IcebergSparkSessionExtensions
│   ├─ Catalog: local (Hadoop)
│   └─ Warehouse: /data/warehouse
│
├─ Query Tables
│   ├─ Database: local.streamify
│   ├─ Tables:
│   │   ├─ listen_events_staging
│   │   ├─ page_view_events_staging
│   │   └─ auth_events_staging
│   └─ SQL syntax: Standard SQL + Iceberg extensions
│
└─ Analysis Examples
    ├─ Top songs: GROUP BY artist, song
    ├─ DAU/MAU: COUNT DISTINCT userId by date
    ├─ Conversion funnel: auth_events + page_view_events
    ├─ Churn analysis: users with auth='Cancelled'
    └─ Time travel: SELECT * FROM table TIMESTAMP AS OF '2024-12-02 15:00:00'
```

## Data Schemas

### Listen Events
```
artist: string
song: string
duration: double (seconds)
ts: long (milliseconds)
auth: string (Guest, Logged In, Logged Out, Cancelled)
level: string (free, paid)
city: string
zip: string
state: string
userAgent: string
lon: double
lat: double
userId: long
lastName: string
firstName: string
gender: string (M, F)
registration: long (milliseconds)
sessionId: int
itemInSession: int
ts_timestamp: timestamp (added by Spark)
year, month, day, hour: int (added by Spark)
load_timestamp: timestamp (added by batch job)
```

### Page View Events
```
ts: long
sessionId: int
auth: string
level: string
itemInSession: int
city, zip, state: string
userAgent: string
lon, lat: double
userId: long
lastName, firstName: string
gender: string
registration: long
page: string (Home, About, Settings, Help, Upgrade, Downgrade, Error, etc.)
+ ts_timestamp, partitions, load_timestamp
```

### Auth Events
```
ts: long
sessionId: int
level: string
itemInSession: int
city, zip, state: string
userAgent: string
lon, lat: double
userId: long
lastName, firstName: string
gender: string
registration: long
success: string (true, false)
+ ts_timestamp, partitions, load_timestamp
```

## Resource Allocation

```
Component            | Memory | CPU | Storage
---------------------|--------|-----|----------
Eventsim             | 4GB    | 1   | <1GB (JAR)
Kafka + Zookeeper    | 2GB    | 1   | 1-5GB (logs)
Spark Master         | 1GB    | 1   | Minimal
Spark Worker         | 2GB    | 2   | Minimal
Data Lake            | -      | -   | 100MB/hour
Iceberg Warehouse    | -      | -   | 120MB/hour
---------------------|--------|-----|----------
Total Recommended    | 8GB+   | 4+  | 10GB+
```

## Monitoring Points

1. **Event Generation Rate**
   - Check: `docker logs eventsim | grep "events generated"`
   - Expected: 100-200 events/second

2. **Kafka Topic Lag**
   - Check: `kafka-consumer-groups --describe`
   - Expected: Near-zero lag (<1000 messages)

3. **Streaming Job Health**
   - Check: Spark UI (http://localhost:8080)
   - Expected: Active application, batches completing in <1 minute

4. **Data Lake Growth**
   - Check: `du -sh /data/lake/`
   - Expected: ~100MB/hour with 10K users

5. **Iceberg Tables**
   - Check: `SELECT COUNT(*) FROM local.streamify.listen_events_staging`
   - Expected: Growing linearly with time

## Failure Scenarios & Recovery

1. **Eventsim Crash**
   - Recovery: `docker-compose restart eventsim`
   - Impact: Gap in data generation
   - Duration: ~10 seconds to restart

2. **Kafka Unavailable**
   - Recovery: `docker-compose restart zookeeper kafka`
   - Impact: Eventsim buffers in memory (limited)
   - Duration: ~30 seconds to restart

3. **Spark Streaming Job Dies**
   - Recovery: Restart spark-submit command
   - Impact: No data loss (Kafka retains messages)
   - Duration: Picks up from last checkpoint

4. **Disk Full**
   - Recovery: Clean old data or expand storage
   - Impact: All writes fail
   - Prevention: Monitor disk usage

## Performance Tuning

### For Higher Throughput
1. Increase Eventsim users: `--nusers 50000`
2. Add more Spark workers (scale out)
3. Reduce batch interval: `trigger="60 seconds"`
4. Increase Kafka partitions (default: 1)

### For Lower Resource Usage
1. Reduce Eventsim users: `--nusers 1000`
2. Increase batch interval: `trigger="300 seconds"`
3. Reduce Spark worker memory: `SPARK_WORKER_MEMORY=1G`
4. Enable Parquet dictionary encoding

---

**Diagram Version**: 1.0  
**Last Updated**: December 2, 2024
