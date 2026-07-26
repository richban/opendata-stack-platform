#!/usr/bin/env bash
##############################################################################
# Lakehouse Cleanup Script
# Removes S3 storage objects and purges Polaris tables/namespaces in 1 call
##############################################################################
set -e

STREAMIFY_DIR="dagster-workspace/projects/streamify"

echo "🧹 Cleaning MinIO S3 storage buckets..."
mc rm --recursive --force local/checkpoints 2>/dev/null || true
mc rm --recursive --force local/lakehouse 2>/dev/null || true

echo "🧹 Purging Apache Polaris namespace & tables (CASCADE)..."
uv run --project "$STREAMIFY_DIR" python -c "
from streamify.defs.resources import create_spark_session, get_streaming_config
spark = create_spark_session(get_streaming_config())
spark.sql('DROP NAMESPACE IF EXISTS lakehouse.streamify CASCADE')
" 2>/dev/null || true

echo "✓ Lakehouse storage and catalog cleanup complete."
