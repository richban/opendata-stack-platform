#!/usr/bin/env bash
##############################################################################
# Lakehouse Cleanup Script
# Removes S3 storage objects and purges Polaris tables/namespaces via Polaris CLI
##############################################################################
set -e

POLARIS_HOST="${POLARIS_HOST:-localhost}"
POLARIS_PORT="${POLARIS_PORT:-8181}"
POLARIS_CLIENT_ID="${POLARIS_BOOTSTRAP_CREDENTIALS_CLIENT_ID:-admin}"
POLARIS_CLIENT_SECRET="${POLARIS_BOOTSTRAP_CREDENTIALS_CLIENT_SECRET:-password}"
CATALOG="${POLARIS_CATALOG:-lakehouse}"
NAMESPACE="${POLARIS_NAMESPACE:-streamify}"
STREAMIFY_DIR="dagster-workspace/projects/streamify"

echo "🧹 Cleaning MinIO S3 storage buckets..."
mc rm --recursive --force local/checkpoints 2>/dev/null || true
mc rm --recursive --force local/lakehouse 2>/dev/null || true

echo "🧹 Purging Apache Polaris catalog tables & namespaces via Polaris CLI..."
# De-register all tables in namespace
tbl_json=$(uv run --project "$STREAMIFY_DIR" polaris --host "$POLARIS_HOST" --port "$POLARIS_PORT" --client-id "$POLARIS_CLIENT_ID" --client-secret "$POLARIS_CLIENT_SECRET" tables list --catalog "$CATALOG" --namespace "$NAMESPACE" 2>/dev/null || true)

if [ -n "$tbl_json" ]; then
  echo "$tbl_json" | grep -o '"name": "[^"]*' | cut -d'"' -f4 | while read -r tbl; do
    if [ -n "$tbl" ]; then
      echo "  ✓ De-registering table $NAMESPACE.$tbl..."
      uv run --project "$STREAMIFY_DIR" polaris --host "$POLARIS_HOST" --port "$POLARIS_PORT" --client-id "$POLARIS_CLIENT_ID" --client-secret "$POLARIS_CLIENT_SECRET" tables delete --catalog "$CATALOG" --namespace "$NAMESPACE" "$tbl" 2>/dev/null || true
    fi
  done
fi

# Delete namespace
echo "  ✓ Deleting namespace $NAMESPACE..."
uv run --project "$STREAMIFY_DIR" polaris --host "$POLARIS_HOST" --port "$POLARIS_PORT" --client-id "$POLARIS_CLIENT_ID" --client-secret "$POLARIS_CLIENT_SECRET" namespaces delete --catalog "$CATALOG" "$NAMESPACE" 2>/dev/null || true

echo "✓ Lakehouse storage and catalog cleanup complete."
