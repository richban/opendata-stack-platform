# Kafka Architecture Documentation

## Overview

This document explains the complete Kafka streaming architecture for the OpenData Stack Platform, including producers, topics, brokers, partitions, and debugging tools.

---

## 1. How Many Producers Do We Have?

**Answer: 4 Kafka Producers**

The eventsim application creates **4 concurrent Kafka producers**:
- `producer-1` → Publishes **auth_events**
- `producer-2` → Publishes **listen_events**  
- `producer-3` → Publishes **page_view_events**
- `producer-4` → Publishes **status_change_events**

### Who Are the Producers?

**The Eventsim Application** is the producer team. Eventsim is a data simulator that generates fake music streaming events (similar to Spotify). It runs as a containerized application (`eventsim` container) and produces realistic event streams.

**Location:** `docker-compose.yml` service `eventsim`
**Source:** Scala application in `/eventsim` directory
**Configuration:** `/eventsim/examples/example-config.json`

---

## 2. What Topics Do We Have?

### Application Topics (4 topics)

| Topic | Purpose | Message Count | Schema |
|-------|---------|---------------|--------|
| `listen_events` | Song play events | 1,365,634 | Song listening activity with artist, song, duration |
| `page_view_events` | Page navigation events | 1,614,002 | User page views (Home, NextSong, Login, etc.) |
| `auth_events` | Authentication events | 20,800 | Login/logout activity |
| `status_change_events` | User status changes | 1,212 | Subscription upgrades/downgrades, cancellations |

### System Topic (1 topic)

| Topic | Purpose | Partitions |
|-------|---------|-----------|
| `__consumer_offsets` | Kafka internal - tracks consumer group offsets | 50 |

---

## 3. What Are the Different Messages?

### listen_events
```json
{
  "artist": "Britt Nicole",
  "song": "Set The World On Fire",
  "duration": 220.36853,
  "ts": 1764183250926,
  "sessionId": 97,
  "auth": "Logged In",
  "level": "free",
  "itemInSession": 0,
  "city": "Sandusky",
  "zip": "44870",
  "state": "OH",
  "userAgent": "Mozilla/5.0...",
  "lon": -82.744814,
  "lat": 41.42828,
  "userId": 42,
  "lastName": "Wilcox",
  "firstName": "Isabelle",
  "gender": "F",
  "registration": 1736811444926
}
```

### page_view_events
```json
{
  "ts": 1764183250926,
  "sessionId": 97,
  "page": "NextSong",
  "auth": "Logged In",
  "method": "PUT",
  "status": 200,
  "level": "free",
  "itemInSession": 0,
  "city": "Sandusky",
  "zip": "44870",
  "state": "OH",
  "userAgent": "Mozilla/5.0...",
  "lon": -82.744814,
  "lat": 41.42828,
  "userId": 42,
  "lastName": "Wilcox",
  "firstName": "Isabelle",
  "gender": "F",
  "registration": 1736811444926,
  "artist": "Britt Nicole",
  "song": "Set The World On Fire",
  "duration": 220.36853
}
```

### auth_events
```json
{
  "ts": 1764187336926,
  "sessionId": 62,
  "level": "paid",
  "itemInSession": 1,
  "city": "Lancaster",
  "zip": "29720",
  "state": "SC",
  "userAgent": "Mozilla/5.0...",
  "lon": -80.732729,
  "lat": 34.740563,
  "userId": 38,
  "lastName": "Mora",
  "firstName": "Roman",
  "gender": "M",
  "registration": 1690397229926,
  "success": true
}
```

### status_change_events
```json
{
  "ts": 1764678149926,
  "sessionId": 128,
  "auth": "Logged In",
  "level": "free",
  "itemInSession": 2,
  "city": "Bear",
  "zip": "19701",
  "state": "DE",
  "userAgent": "Mozilla/5.0 (iPhone...)",
  "lon": -75.700909,
  "lat": 39.583153,
  "userId": 29,
  "lastName": "Jones",
  "firstName": "Yuki",
  "gender": "M",
  "registration": 1744672710926
}
```

---

## 4. How Is This Configured?

### Eventsim Producer Configuration

**File:** `docker-compose.yml`

```yaml
eventsim:
  command:
    - |
      /opt/eventsim/eventsim.sh \
        -c /opt/eventsim/examples/example-config.json \
        --from 30 \
        --nusers 1000 \
        --growth-rate 0.10 \
        --userid 1 \
        --kafkaBrokerList kafka:9092 \
        --randomseed $$(shuf -i 1-10000 -n 1) \
        --continuous
```

**Key Parameters:**
- `--kafkaBrokerList kafka:9092` → Kafka broker endpoint
- `--nusers 1000` → Simulate 1,000 users
- `--from 30` → Generate data from 30 days ago
- `--continuous` → Continuous event generation mode
- `--growth-rate 0.10` → 10% annual user growth

### Kafka Producer Settings

**From eventsim logs:**
```
acks = 1
batch.size = 16384
bootstrap.servers = [kafka:9092]
buffer.memory = 33554432
```

**Configuration Source:** `/eventsim/examples/example-config.json`
- Defines page state machine (transitions between pages)
- User behavior probabilities
- Session parameters (alpha=90, beta=604800)
- Damping factors for daily/weekend cycles

---

## 5. How Many Brokers Do We Have?

**Answer: 1 Kafka Broker**

| Broker ID | Address | Rack | Role |
|-----------|---------|------|------|
| 1 | kafka:9092 | null | Leader for all partitions |

**Configuration:** `docker-compose.yml` service `kafka`

```yaml
kafka:
  image: confluentinc/cp-kafka:7.5.0
  environment:
    KAFKA_BROKER_ID: 1
    KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:9093
```

**Note:** This is a single-node cluster suitable for development/testing. Production would use 3+ brokers.

---

## 6. Do We Have Partitions?

**Answer: Yes, Each Topic Has 1 Partition**

| Topic | Partition | Leader | Replicas | ISR |
|-------|-----------|--------|----------|-----|
| listen_events | 0 | 1 | 1 | 1 |
| page_view_events | 0 | 1 | 1 | 1 |
| auth_events | 0 | 1 | 1 | 1 |
| status_change_events | 0 | 1 | 1 | 1 |

**Terminology:**
- **Partition:** Ordered, immutable sequence of records
- **Leader:** Broker that handles all reads/writes for a partition
- **Replicas:** Number of copies of the partition (1 = no replication)
- **ISR (In-Sync Replicas):** Replicas that are caught up with the leader

**Current Setup:**
- PartitionCount: 1 per topic
- ReplicationFactor: 1 (no redundancy)
- All partitions led by Broker 1

---

## 7. Do We Have a Leader?

**Answer: Yes, Broker 1 is the Leader**

Broker 1 (kafka:9092) is the **leader** for all 4 topic partitions. In Kafka:

- **Leader:** Handles all read/write requests for a partition
- **Follower:** Replicates data from leader (we have none currently)
- **Controller:** Manages cluster metadata (also Broker 1)

**Check leadership:**
```bash
docker exec opendata-stack-platform-kafka-1 kafka-topics --describe --bootstrap-server localhost:9092
```

---

## 8. Do We Acknowledge Messages?

**Answer: Yes, with `acks=1`**

### Acknowledgment Level: `acks=1`

**Configuration from producer:**
```
acks = 1
```

**What this means:**
- Producer waits for **leader** acknowledgment only
- Leader writes to local log and acknowledges immediately
- Does NOT wait for replicas to acknowledge (we have no replicas anyway)

### Acknowledgment Levels Explained

| Level | Behavior | Durability | Latency |
|-------|----------|------------|---------|
| `acks=0` | No acknowledgment | Lowest | Fastest |
| `acks=1` | Leader acknowledgment (current) | Medium | Medium |
| `acks=all` | All in-sync replicas acknowledge | Highest | Slowest |

**Trade-off:** With `acks=1` and no replication, if Broker 1 fails before replicas sync, messages could be lost. For production, use `acks=all` with replication factor ≥ 3.

---

## 9. How Can We Debug This?

### Quick Debug Commands

#### 1. Check Cluster Health
```bash
docker ps | grep -E "(kafka|zookeeper|eventsim)"
```

#### 2. List All Topics
```bash
docker exec opendata-stack-platform-kafka-1 kafka-topics --list --bootstrap-server localhost:9092
```

#### 3. Get Topic Details (Partitions, Leaders, Replicas)
```bash
docker exec opendata-stack-platform-kafka-1 kafka-topics --describe --bootstrap-server localhost:9092
```

#### 4. Count Messages in Topic
```bash
docker exec opendata-stack-platform-kafka-1 kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic listen_events
```

#### 5. Consume Messages in Real-Time
```bash
docker exec opendata-stack-platform-kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic listen_events \
  --from-beginning
```

#### 6. Check Producer Logs
```bash
docker logs -f eventsim
```

#### 7. Check Broker Logs
```bash
docker logs -f opendata-stack-platform-kafka-1
```

#### 8. Check Consumer Groups
```bash
docker exec opendata-stack-platform-kafka-1 kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list
```

#### 9. View Broker Configuration
```bash
docker exec opendata-stack-platform-kafka-1 kafka-configs \
  --bootstrap-server localhost:9092 \
  --entity-type brokers \
  --entity-name 1 \
  --describe
```

#### 10. Check Broker API Versions
```bash
docker exec opendata-stack-platform-kafka-1 kafka-broker-api-versions \
  --bootstrap-server localhost:9092
```

---

## 10. How Can I See This?

### Visualize Messages

#### Sample Message from Each Topic
```bash
for topic in listen_events page_view_events auth_events status_change_events; do
  echo "=== $topic ==="
  docker exec opendata-stack-platform-kafka-1 kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic $topic \
    --max-messages 1 \
    --from-beginning 2>/dev/null | jq .
done
```

#### Monitor Live Event Stream
```bash
# Terminal 1: Watch listen events
docker exec opendata-stack-platform-kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic listen_events

# Terminal 2: Watch page view events
docker exec opendata-stack-platform-kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic page_view_events
```

#### Tail Last 10 Messages
```bash
docker exec opendata-stack-platform-kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic listen_events \
  --max-messages 10 \
  --partition 0 \
  --offset latest
```

### Monitor Producers

#### Check Active Producers
```bash
docker logs eventsim 2>&1 | grep "Starting Kafka producer" | tail -10
```

#### See Producer Performance
```bash
docker logs eventsim 2>&1 | grep "Events:" | tail -10
```

Example output:
```
Now: 2025-11-14T11:18:36, Events:10000, Rate: 28089 eps
Now: 2025-11-19T08:57:34, Events:20000, Rate: 93457 eps
```

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    Eventsim Container                       │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Producer-1 ──────► auth_events                      │   │
│  │  Producer-2 ──────► listen_events                    │   │
│  │  Producer-3 ──────► page_view_events                 │   │
│  │  Producer-4 ──────► status_change_events             │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────┬───────────────────────────────────────┘
                      │ kafka:9092
                      ▼
┌─────────────────────────────────────────────────────────────┐
│              Kafka Broker (ID: 1)                           │
│  ┌────────────────────────────────────────────────────┐     │
│  │  Topic: listen_events          │ Partition 0       │     │
│  │  Topic: page_view_events       │ Partition 0       │     │
│  │  Topic: auth_events            │ Partition 0       │     │
│  │  Topic: status_change_events   │ Partition 0       │     │
│  └────────────────────────────────────────────────────┘     │
│                                                             │
│  Acknowledgment: acks=1                                     │
│  Replication Factor: 1                                      │
│  Leader: Broker 1 (for all partitions)                      │
└──────────────────┬──────────────────────────────────────────┘
                   │ zookeeper:2181
                   ▼
┌─────────────────────────────────────────────────────────────┐
│                   Zookeeper                                 │
│  - Cluster metadata                                         │
│  - Broker coordination                                      │
│  - Leader election                                          │
└─────────────────────────────────────────────────────────────┘
```

---

## Current Status Summary

| Metric | Value |
|--------|-------|
| **Brokers** | 1 |
| **Producers** | 4 (eventsim) |
| **Topics** | 4 (+ 1 internal) |
| **Partitions per Topic** | 1 |
| **Replication Factor** | 1 |
| **Total Messages** | 3,001,648 |
| **Acknowledgment Mode** | acks=1 |
| **Leader** | Broker 1 |

---

## Production Recommendations

1. **Increase Brokers:** Use 3+ brokers for fault tolerance
2. **Increase Replication:** Set `replication.factor=3`
3. **Increase Partitions:** Enable parallelism and higher throughput
4. **Use `acks=all`:** Ensure all replicas acknowledge writes
5. **Add Monitoring:** Use Kafka Manager, Confluent Control Center, or Prometheus
6. **Enable Retention:** Configure retention policies for data lifecycle

---

## Debugging Script

Save this script as `kafka-debug.sh`:

```bash
#!/bin/bash
echo "=== KAFKA CLUSTER STATUS ==="
docker ps | grep -E "(kafka|zookeeper|eventsim)"

echo -e "\n=== TOPICS ==="
docker exec opendata-stack-platform-kafka-1 kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null

echo -e "\n=== MESSAGE COUNTS ==="
for topic in listen_events page_view_events auth_events status_change_events; do
  count=$(docker exec opendata-stack-platform-kafka-1 kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic $topic 2>/dev/null | awk -F':' '{print $3}')
  printf "%-25s %10s messages\n" "$topic:" "$count"
done

echo -e "\n=== TOPIC DETAILS ==="
docker exec opendata-stack-platform-kafka-1 kafka-topics --describe --bootstrap-server localhost:9092 2>/dev/null | grep -v "__consumer_offsets"
```

Run with: `bash kafka-debug.sh`
