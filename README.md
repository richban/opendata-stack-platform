#  Opendata Stack: 🚕🗽  📊  NYC TLC Trip Record Data 

## Overview

### Business Process

#### Process Being Measured
Taxi and For-Hire Vehicle trips in NYC, segmented by Yellow Taxi, Green Taxi, and FHV data.

#### Goals
- Facilitate granular analysis across different taxi types.
- Track trip performance, service coverage, and revenue.
- Compare trip patterns between Yellow Taxi, Green Taxi, and FHV.

#### Business Value
- **TLC Operations**: Evaluate service performance by fleet type.
- **City Planning**: Insights into urban mobility and congestion patterns.
- **Taxi Companies**: Optimize fleet allocation and identify underserved areas.
- **FHV Companies**: Improve shared-ride services and evaluate market coverage.

#### Target Audience
- NYC Taxi and Limousine Commission
- Yellow Taxi and Green Taxi operators
- FHV companies (e.g., Uber, Lyft)
- Urban policymakers and transportation analysts

---

## Engineering Vision

This is a **Work In Progress (WIP)** project to visualize and analyze **NYC Taxi Trips** data using modern, open-source data tooling.

The primary goal of this project is to develop a **modern data stack pipeline** using the tools listed below. The design emphasizes:

- **Multi-Engine Stack**: Utilize **DuckDB** locally for fast prototyping and lightweight analysis, and **Snowflake** in production for scalability and performance.
- **Open Table Formats**: Incorporate **Delta Lake** or other open table formats to ensure compatibility, flexibility, and performance.
- **Open Catalogs**: Leverage **open metadata catalogs** for better discoverability, governance, and interoperability.


### 🧰 Tools Planned

- **Orchestration**: [**Dagster**](https://github.com/dagster-io/dagster) To orchestrate and monitor the pipeline, ensuring tasks are executed reliably and dependencies are managed effectively.
- **Data Warehouse**:
    - [**DuckDB**](https://github.com/duckdb/duckdb): A high-performance SQL OLAP engine for local and small-scale analysis.
    - **Snowflake**: For production-scale analytics, providing scalability and enterprise-grade performance.
- **Extract & Load**: [**dlt**](https://github.com/dlt-hub/dlt) To extract raw data from NYC’s Open Data portal (Yellow, Green, and HVFHV taxi trips) and load it into the data warehouse.
- **Transform**: [**dbt**](https://github.com/dbt-labs/dbt-core) For building modular, reusable, and version-controlled transformations in SQL, enabling robust data modeling of NYC Taxi data.
- **Business Intelligence (BI)**: [**Evidence**](https://github.com/evidence-dev/evidence) A modern, lightweight BI tool for creating visually appealing and shareable reports about taxi trips and trends.

