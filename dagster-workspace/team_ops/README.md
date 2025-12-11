# Team Ops - Music Streaming Pipeline (Streamify)

Real-time music streaming data pipeline using Eventsim, Kafka, Spark Streaming, and Iceberg. Simulates a Spotify-like service with 10,000 users generating listen events, page views, and authentication events.

> **📖 Quick Start**: See [`/QUICKSTART.md`](../../QUICKSTART.md) for immediate setup  
> **📚 Full Guide**: See [`/SETUP.md`](../../SETUP.md) for detailed documentation

## Architecture

```
Eventsim → Kafka (3 topics) → Spark Streaming → Data Lake (Parquet, 2-min intervals)
                                                         ↓
                                              Hourly Batch → Iceberg Warehouse
```

## Data Flow

1. **Eventsim** generates fake music streaming events (10K users, continuous)
2. **Kafka** receives events on 3 topics: `listen_events`, `page_view_events`, `auth_events`
3. **Spark Streaming** consumes from Kafka → writes to data lake every 2 minutes (partitioned by month/day/hour)
4. **Hourly Batch** loads parquet files from data lake → Iceberg warehouse for analytics

## Event Types

### Listen Events (NextSong)
- Artist, song, duration, timestamp
- User: ID, name, gender, level (free/paid), location
- Session: sessionId, itemInSession, userAgent

### Page View Events
- Page: Home, About, Settings, Help, Upgrade, etc.
- User demographics and session info

### Auth Events
- Actions: Login, Logout, Register, Cancelled
- Success status and user details

## Setup

### 1. Start Infrastructure

```bash
docker-compose up -d
```

**Services Started:**
- Kafka + Zookeeper (ports 9092, 9093)
- Spark Master + Worker (port 8080)
- Eventsim (auto-starts generating events)
- MinIO (object storage)

### 2. Install Dependencies

```bash
cd dagster-workspace/team_ops
uv sync
source .venv/bin/activate
```

## Usage

### Monitor Event Generation

Check Kafka topics:
```bash
docker exec -it opendata-stack-platform-kafka-1 \
  kafka-topics --list --bootstrap-server localhost:9092
```

Expected topics: `listen_events`, `page_view_events`, `auth_events`

### Run Spark Streaming (Data Lake Writer)

Writes parquet files every 2 minutes to `/data/lake`:

```bash
docker exec -it opendata-stack-platform-spark-master-1 \
  spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  /opt/team_ops/src/team_ops/stream_music_events.py
```

### Run Hourly Batch Load

Load specific hour's data from lake to Iceberg warehouse:

```bash
docker exec -it opendata-stack-platform-spark-master-1 \
  spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1 \
  /opt/team_ops/src/team_ops/hourly_batch_load.py \
  --year 2024 \
  --month 12 \
  --day 2 \
  --hour 15
```

### Run Deduplication (Optional)

Deduplicate events by sessionId + itemInSession:

```bash
docker exec -it opendata-stack-platform-spark-master-1 \
  spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1 \
  /opt/team_ops/src/team_ops/batch_compaction.py \
  --source-table listen_events_staging \
  --target-table listen_events_gold
```

## Data Storage

### Data Lake (`/data/lake/`)
```
/data/lake/
├── listen_events/
│   └── month=12/day=2/hour=15/*.parquet
├── page_view_events/
│   └── month=12/day=2/hour=15/*.parquet
└── auth_events/
    └── month=12/day=2/hour=15/*.parquet
```

### Iceberg Warehouse (`/data/warehouse/`)
```
/data/warehouse/streamify/
├── listen_events_staging/
├── page_view_events_staging/
└── auth_events_staging/
```

## Key Features

- **Real-time Processing**: 2-minute micro-batches
- **Partitioned Storage**: Month/day/hour partitions for efficient queries
- **Schema Evolution**: Iceberg supports schema changes
- **Time Travel**: Query historical snapshots with Iceberg
- **Scalable**: 10K users generating ~100+ events/sec

## Analytics Queries

Query listen events from Iceberg using Spark SQL:

```python
spark.sql("""
    SELECT artist, song, COUNT(*) as play_count
    FROM local.streamify.listen_events_staging
    WHERE ts_timestamp >= current_timestamp() - INTERVAL 1 HOUR
    GROUP BY artist, song
    ORDER BY play_count DESC
    LIMIT 10
""").show()
```

Popular songs, active users, demographics analysis possible with SQL on Iceberg tables.

## Monitoring

- **Spark UI**: http://localhost:8080
- **Kafka Topics**: `docker exec opendata-stack-platform-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic listen_events --from-beginning --max-messages 10`
- **Eventsim Logs**: `docker logs eventsim`

## Configuration

### Eventsim Settings (docker-compose.yml)
- `--nusers 10000`: Number of simulated users
- `--growth-rate 10`: User growth rate
- `--continuous`: Run continuously

### Spark Streaming Settings
- Trigger interval: 120 seconds (2 minutes)
- Output format: Parquet
- Checkpoint location: `/data/checkpoints/`

## Troubleshooting

**No events in Kafka:**
```bash
docker restart eventsim
docker logs eventsim
```

**Streaming job fails:**
- Check Spark logs: `docker logs opendata-stack-platform-spark-master-1`
- Verify Kafka connectivity: `kafka:9092` from inside Spark container

**Hourly batch finds no data:**
- Verify data lake path exists: `docker exec opendata-stack-platform-spark-master-1 ls -la /data/lake/listen_events/`
- Check month/day/hour partitions match your arguments
