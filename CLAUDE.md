# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a modern data analytics platform for NYC taxi trip data that demonstrates end-to-end data engineering practices. The platform uses a multi-workspace architecture with Dagster orchestration, dbt transformations, and supports both DuckDB (local) and Snowflake (production) as data warehouses.

## Architecture

### Core Components

- **Dagster workspace** (`dagster-workspace/`): Multi-team orchestration setup with:
  - `dagster-workspace/team_data_platform/data_platform/`: Main data platform with taxi data pipelines
  - `dagster-workspace/team_climate_risk/physical_risk_impact/`: Climate risk analysis workspace
  - `dagster-workspace/team_ops/`: Streaming data pipelines
  - `dagster-workspace/team_ml/`: Bauplan for ML pipelines
- **sqlmesh project** (`opendata_stack_platform_sqlmesh/`): Data transformations with bronze/silver/gold architecture
- **Infrastructure** (`docker-compose.yml`): MinIO S3 storage, Apache Polaris catalog, and supporting services

### Data Flow Architecture

1. **Bronze Layer**: Raw data ingestion via dlt (Data Load Tool)
2. **Silver Layer**: Cleaned and validated data via dbt
3. **Gold Layer**: Business-ready dimensional models and marts
4. **Storage**: Open table formats (Iceberg) with MinIO S3-compatible storage
5. **Catalog**: Apache Polaris for metadata management

## Common Commands

### Testing and Quality
```bash
# Run tests
pytest

# Run linting and formatting
ruff check .
ruff format .
```

### Dagster Operations
```bash
# Run from dagster workspace directory
cd dagster-workspace & dg dev
```

### dbt Operations
```bash
# sqlmesh commands (run from project root)

# Direct sqlmesh usage
cd opendata_stack_platform_sqlmesh
echo "y" | sqlmesh plan
```

### Infrastructure
```bash
# Start services
docker-compose up -d

# Set up Polaris catalog
./setup_polaris.sh
```

## Development Notes

### Package Management
- Uses `uv` for Python package management
- Two main Python projects with separate `pyproject.toml` files:
  - `dagster-workspace/team_data_platform/data_platform/pyproject.toml`
  - `dagster-workspace/team_climate_risk/physical_risk_impact/pyproject.toml`
- each project has  it's own dependencies and virtual environment  `.venv`

### Code Quality
- **Ruff** for linting and formatting (configured in `ruff.toml`)
- Line length: 90 characters
- Python 3.9+ target version
- Pre-commit hooks configured

### Testing
- pytest for unit testing
- Tests located in `dagster-workspace/team_data_platform/data_platform/tests/`
- Run tests with `pytest`

### Data Platform Specifics
- **Dagster assets** define data pipeline steps
- **sqlmesh models** handle data transformations
- **DuckDB** used for local development
- **Partitioning** strategies implemented for time-based data
- **Data quality** tests in both Dagster and dbt layers
- **DuckLake lakehouse** for multi-user access to data marts

### DuckLake Multi-User Lakehouse
- **Purpose**: Enable multiple analysts to query data marts simultaneously
- **Storage**: Data marts exported to DuckLake format in S3
- **Access**: Any DuckDB instance can read DuckLake tables using `ducklake_scan()`
- **Pipeline**: Dagster assets export dbt data marts to DuckLake after transformation
- **Tables**: `mart_revenue_analysis`, `mart_zone_analysis` available in `s3://datalake/marts/`

### Environment Configuration
- Uses `.env` files for environment variables
- Docker Compose manages service dependencies
- MinIO provides S3-compatible storage
- Apache Polaris handles table catalog management

## Key Directories

- `dagster-workspace/`: Dagster orchestration code
- `opendata_stack_platform_dbt/`: dbt transformation models (LEGACY NOT MAINTAINED)
- `opendata_stack_platform_sqlmesh/`: sqlmesh transformation models
- `data/`: Local data storage and DuckDB files, Dagster home files,  other static data
- `polaris-config/`: Apache Polaris catalog configuration
- `minio-config/`: MinIO S3 storage configuration
- `docs/`: Documentation and data model diagrams

## Data Model

The platform implements a dimensional model with:
- **Fact table**: `fact_taxi_trip` (trip-level metrics)
- **Dimension tables**: date, location, payment type, vendor, etc.
- **Marts**: Revenue analysis, geographic analysis
- **Bronze/Silver/Gold** layered architecture for data quality

## Important Files

- `docker-compose.yml`: Service orchestration
- `ruff.toml`: Code quality configuration
- `dagster-workspace/dg.toml`: Dagster workspace configuration
- `opendata_stack_platform_dbt/dbt_project.yml`: dbt project settings
- `opendata_stack_platform_sqlmesh/config.yml`: sqlmesh project settings
- `.envrc` based  on the current setting the proper environment is initialized
