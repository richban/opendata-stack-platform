# Open Data Stack Platform

A modern, production-grade data platform demonstrating end-to-end data engineering, real-time streaming, dimensional modeling, machine learning, and geospatial analytics across a **multi-project Dagster workspace**.


## Monorepo Architecture & Projects

The platform is organized into four domain-specific projects orchestrated through a unified Dagster workspace (`dg.toml`), isolating project dependencies while enabling shared lineage, monitoring, and cross-project asset execution:

```mermaid
flowchart TD
    INFRA[Docker Infrastructure\nMinIO · Polaris · Kafka · Redis · ClickHouse · Spark]
    
    subgraph Projects [Dagster Multi-Project Workspace]
        P1[data_platform\nNYC TLC · dlt · SQLMesh · DuckDB]
        P2[streamify\nReal-time Streaming · PySpark · ClickHouse · Iceberg]
        P3[ml_regression_model\nFeature Engineering · Model Training]
        P4[physical_risk_impact\nGeospatial Analytics · Climate Risk]
    end

    INFRA --> P1
    INFRA --> P2
    INFRA --> P3
    INFRA --> P4
```

### 📦 Workspace Projects

| Project | Domain / Scope | Key Technologies | Description |
| :--- | :--- | :--- | :--- |
| [**`data_platform`**](./dagster-workspace/projects/data_platform) | NYC TLC Taxi Batch Analytics | `dlt`, `SQLMesh`, `dbt`, `DuckDB`, `Snowflake`, `Dagster` | Ingests, models, and transforms billions of NYC taxi records into dimensional data marts with monthly partition backfills. |
| [**`streamify`**](./dagster-workspace/projects/streamify) | Real-Time Music Streaming Ingestion | `Kafka`, `ksqlDB`, `PySpark 4.0`, `Redis`, `ClickHouse`, `Iceberg` | Enterprise streaming pipeline (500K events/s) with schema validation, Iceberg DLQ table, executor-side Redis enrichment, and dual ClickHouse/Iceberg sinks. |
| [**`ml_regression_model`**](./dagster-workspace/projects/ml_regression_model) | Machine Learning & Feature Engineering | `Python`, `Scikit-Learn`, `Polars`, `Dagster` | Predictive modeling pipelines for fare estimation and trip duration analysis. |
| [**`physical_risk_impact`**](./dagster-workspace/projects/physical_risk_impact) | Climate & Geospatial Risk Analysis | `Polars`, `Geospatial libraries`, `Dagster` | Assessment and vulnerability modeling of physical asset risk against climate data. |


## 🛠️ Technology Stack & Tooling

- **Orchestration**: **Dagster** (`dg` multi-project workspace, Software-Defined Assets, Sensors, Partitions).
- **Transformations & Modeling**: **SQLMesh** *(Preferred — virtual environments, column-level lineage)* & **dbt** *(Legacy)*.
- **Compute & Engines**: **PySpark 4.0 (Spark Connect)**, **Polars**, **DuckDB** (local OLAP), **ClickHouse** (real-time streaming OLAP), and **Snowflake** (production cloud warehouse).
- **Lakehouse & Storage**: **Apache Iceberg** table format, **Apache Polaris** REST Catalog, and **MinIO** S3-compatible object storage.
- **Streaming & Cache**: **Apache Kafka**, **Confluent Schema Registry**, **ksqlDB**, and **Redis 7**.
- **Package Management**: **`uv`** for fast, deterministic Python virtual environment and dependency management.


## 🐳 Shared Infrastructure (`docker-compose.yml`)

The platform includes a root Docker Compose environment providing shared infrastructure services. Individual workspace projects consume only the services they require:

```bash
docker compose up -d
```

### Important Files

- `docker-compose.yml`: Services orchestration
- `dagster-workspace/dg.toml`: Dagster workspace configuration
- `opendata_stack_platform_dbt/dbt_project.yml`: dbt project settings
- `opendata_stack_platform_sqlmesh/config.yml`: sqlmesh project settings
- `.envrc` based  on the current setting the proper environment is initialized

### Access Points & Service URLs

| Service | Port / URL | Purpose |
| :--- | :--- | :--- |
| **Dagster UI** | `http://localhost:3000` | Unified data orchestration, asset lineage, and run monitoring |
| **Polaris Console** | `http://localhost:3002` | Web UI for Apache Polaris Iceberg catalog & namespace management |
| **Polaris REST API** | `http://localhost:8181` | REST Catalog API for Iceberg table operations |
| **MinIO Console** | `http://localhost:9001` | S3-compatible object storage management (`minioadmin` / `minioadmin`) |
| **Spark Master UI** | `http://localhost:8080` | Spark cluster management & active executor inspection |
| **Spark Connect** | `sc://localhost:15002` | Remote gRPC execution for PySpark streaming jobs |
| **ClickHouse HTTP** | `http://localhost:8123` | Fast-path analytical SQL query interface |
| **Kafdrop** | `http://localhost:9002` | Kafka topic, partition, and consumer lag inspector |
| **Schema Registry** | `http://localhost:8081` | Centralized Avro/JSON schema registry |
| **ksqlDB** | `http://localhost:8088` | Stream query processing & user profile generation |
| **Redis** | `localhost:6379` | In-memory caching for executor-side streaming lookups |

## Getting Started

### Shared Infrastructure

```bash
docker compose up -d
```

### Launch the Multi-Project Dagster Workspace

```bash
cd dagster-workspace
dg dev
```

Open `http://localhost:3000` to view and materialize assets across all four projects.

### Running Project-Specific Workflows

#### Execute SQLMesh transformations

```bash
cd ./opendata_stack_platform_sqlmesh
sqlmesh plan && sqlmesh run
```

#### Streamify Real-Time Ingestion
```bash
cd dagster-workspace/projects/streamify
uv sync && source .venv/bin/activate

# Run Redis cache seeding & streaming pipeline
python -m streamify.seed_redis
python -m streamify.main
```

