#  Opendata ...


## 🚀 Quick Start

### Infrastructure Services

> 📌 **Prerequisite**: `eventsim` requires `eventsim/target/eventsim-assembly-2.0.jar` to be built prior to running `docker compose up`. See [eventsim/README.md](./eventsim/README.md#building-eventsim-assembly-20jar-manually) for instructions.

Start all services with Docker Compose:

```bash
docker-compose up -d
```

This starts:
- **MinIO** (S3-compatible storage): Ports 9000 (API), 9001 (Console)
- **Apache Polaris** (Iceberg catalog): Ports 8181 (API), 8182 (Management)
- **Polaris Console** (Web UI): Port 3001
- **Kafka + Zookeeper**: Port 9092 (broker)
- **Spark** (Master + Worker + Connect): Ports 8080 (UI), 7077 (Master), 15002 (Connect)
- **Eventsim** (Event generator): Generates streaming events to Kafka

### Access Points

| Service | URL | Description |
|---------|-----|-------------|
| Polaris Console | http://localhost:3001 | Web UI for Apache Polaris catalog management |
| Polaris API | http://localhost:8181 | REST API for Iceberg catalog operations |
| MinIO Console | http://localhost:9001 | S3 storage web interface (minioadmin/minioadmin) |
| Spark Master UI | http://localhost:8080 | Monitor Spark cluster and jobs |
| Dagster UI | http://localhost:3000 | Orchestration and pipeline monitoring |

### Polaris Console

The Polaris Console provides a modern web interface for managing Apache Polaris:
- Browse catalogs, namespaces, and tables
- View table schemas and metadata
- Manage access policies and principals
- Monitor catalog operations

**Login Credentials**: Use the OAuth credentials from `dagster-workspace/streamify/.env.polaris`


## Architecture

### Key Components

- **Eventsim**: Generates realistic user activity events
- **Kafka**: Event streaming platform
- **Spark Connect**: Remote Spark execution without subprocess overhead
- **Apache Polaris**: Open-source Iceberg REST catalog with governance
- **MinIO**: S3-compatible object storage for Iceberg data files
- **Dagster**: Data orchestration and monitoring
