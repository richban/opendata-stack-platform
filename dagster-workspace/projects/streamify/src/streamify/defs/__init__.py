"""Dagster definitions for the Streamify lakehouse pipeline.

This package contains:
- assets: Silver layer batch processing assets (dedup, sessionize, maintenance)
- resources: Spark, Polaris, MinIO configuration
- definitions: Main Dagster definitions with jobs and schedules

The streaming component (Kafka -> Iceberg) runs independently via spark-submit.
Dagster orchestrates the batch layer only.
"""
