"""Spark scripts for the Streamify lakehouse pipeline.

This package contains PySpark scripts that run on the Spark cluster:

Streaming:
- stream_to_iceberg.py: Kafka -> Iceberg Bronze tables (runs continuously)

Batch (orchestrated by Dagster):
- create_bronze_tables.py: DDL for Bronze Iceberg tables
- deduplicate_events.py: Deduplication for Silver layer
- sessionize_events.py: Session stitching
- compact_tables.py: Iceberg file compaction
- expire_snapshots.py: Snapshot expiry (7-day retention)

Usage:
    spark-submit --master spark://spark-master:7077 \\
        --packages <packages> \\
        /path/to/script.py [args]
"""
