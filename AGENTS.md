# AGENTS.md — AI Agent Guidance & Repository Rules

This document provides instructions, context, and architectural invariants for AI coding agents working in the `opendata-stack-platform` monorepo.

---

## 1. Repository Architecture & Layout

This repository is a **multi-project modern data platform** managed via Dagster:

- `dagster-workspace/projects/data_platform`: NYC TLC taxi batch pipeline (`dlt`, `DuckDB`/`Snowflake`, `SQLMesh`).
- `dagster-workspace/projects/streamify`: Real-time streaming pipeline (`Kafka`, `PySpark 4.0`, `Redis`, `ClickHouse`, `Iceberg`).
- `dagster-workspace/projects/ml_regression_model`: ML regression & fare estimation pipeline.
- `dagster-workspace/projects/physical_risk_impact`: Geospatial climate risk analysis.
- `opendata_stack_platform_sqlmesh/`: **Preferred** transformation layer.
- `opendata_stack_platform_dbt/`: **Legacy** transformation layer (do not add new models here).

---

## 2. Core Development Invariants

- **Package Management**: Always use `uv` for dependency management. Each project under `dagster-workspace/projects/<name>` has its own `pyproject.toml` and `.venv`. Never install packages globally.
- **Transformations**: Always use **SQLMesh** for data transformations unless explicitly asked to maintain legacy dbt models.
- **Spark Execution**: PySpark runs via **Spark Connect** (`sc://localhost:15002`) in Docker or remote sessions.
- **Dead-Letter Queue (DLQ)**: In Streamify, malformed events are written directly to the **Iceberg DLQ table** (`dlq_events_ingestion`), NOT a Kafka topic.
- **Code Quality**: Code must adhere to **Ruff** linting and formatting standards (`ruff check .`, `ruff format .`).

---

## 3. Essential Commands

### Environment & Workspace
```bash
# Start all shared Docker services
docker compose up -d

# Run Dagster multi-project workspace
cd dagster-workspace
dg dev # or: dagster dev
```

### Testing & Code Quality
```bash
# Run tests
pytest

# Format & Lint
ruff check . --fix
ruff format .
```

### SQLMesh Operations
```bash
cd opendata_stack_platform_sqlmesh
sqlmesh plan
sqlmesh run
```

### Streamify Operations
```bash
cd dagster-workspace/projects/streamify
source .venv/bin/activate
python -m streamify.seed_redis
python -m streamify.main
```
