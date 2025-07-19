# Opendata Stack Platform - SQLMesh Project

This project demonstrates a modern data warehousing solution using SQLMesh to process and analyze NYC taxi trip data. It follows a structured approach to transform raw data into analytics-ready data marts.

## Overview

The core of this project is a data pipeline orchestrated by SQLMesh. The pipeline ingests raw data, transforms it through several layers, and produces final data marts for business intelligence and analytics. The entire process is built on a stack of DuckDB for processing, MinIO for S3-compatible storage, and DuckLake for managing the data lakehouse.

## Data Pipeline Architecture

The pipeline is structured into several distinct layers:

### 1. Data Ingestion (External Source)

- **Tool:** `dlt` (Data Load Tool)
- **Process:** Raw taxi trip data (from Yellow, Green, and FHVHV services) is ingested from source files into a `bronze` layer within our DuckDB database.
- **Integration Note:** Currently, the `dlt` ingestion process runs independently of SQLMesh. The `bronze` tables are treated as **external sources** by SQLMesh.

### 2. Silver Layer: Staging & Validation

- **Purpose:** The `silver` layer takes the raw bronze data, cleans it, applies validation rules, and standardizes it into a unified format.
- **Key Models:**
    - `silver_taxi_trips`: Combines data from all taxi types, cleans columns, and applies initial data quality checks.
    - `silver_taxi_trips_validated`: Filters for only the valid records from the previous step, ensuring downstream models work with clean data.

### 3. Gold Layer: Kimball Data Warehouse

- **Purpose:** The `gold` layer models the data into a classic Kimball-style dimensional warehouse. This provides a structured, easy-to-query schema for analytics.
- **Dimensions:**
    - `dim_date`: A comprehensive date dimension.
    - `dim_location`: Geographic data for taxi zones.
    - `dim_vendor`: Information about the taxi vendors.
    - `dim_payment_type`, `dim_rate_code`, `dim_trip_type`: Dimensions for various trip attributes.
- **Facts:**
    - `fact_taxi_trip`: The central fact table containing foreign keys to the dimensions and all the key metrics of a taxi trip (fares, distance, etc.).

### 4. Analytics Marts (DuckLake)

- **Purpose:** The final layer consists of aggregated data marts designed for specific analytical use cases. These marts are stored in the **DuckLake** catalog, which is backed by our S3 storage (MinIO).
- **Key Models:**
    - `mart_zone_analysis`: Aggregates data for geographic analysis of trip patterns.
    - `mart_revenue_analysis`: Provides a detailed breakdown of revenue metrics.

## Technology Stack

- **Orchestration & Transformation:** SQLMesh
- **Data Processing Engine:** DuckDB
- **Lakehouse Storage:** DuckLake
- **Object Storage:** MinIO (S3 compatible)
- **Ingestion:** dlt

## How to Run

1.  **Set up the environment:** Ensure Docker is running and the required services (`minio`, etc.) are up.
2.  **Plan changes:** To see the current state and plan any model changes, run:
    ```bash
    sqlmesh plan dev
    ```
3.  **Apply and backfill:** To apply the plan and run the models, use:
    ```bash
    sqlmesh run
    ```
4.  **Querying the data:** Connect to the DuckDB database to query the silver and gold layers. To query the final marts, connect to the DuckLake catalog as demonstrated in previous interactions.

```sql
INSTALL httpfs;
LOAD httpfs;
INSTALL aws;
LOAD aws;
INSTALL ducklake;
LOAD ducklake;

SET s3_region = 'us-east-1';
SET s3_access_key_id = 'miniouser';
SET s3_secret_access_key = 'miniouser';
SET s3_endpoint = '127.0.0.1:9000';
SET s3_use_ssl = false;
SET s3_url_style = 'path';


ATTACH 'ducklake:./data/analytics_metadata.ducklake' AS ducklake (DATA_PATH 's3://warehouse');

```
