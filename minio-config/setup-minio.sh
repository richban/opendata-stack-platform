#!/bin/sh
set -e

echo "Setting up MinIO..."

mc alias set minio http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

# Create buckets for different purposes
mc mb minio/warehouse --ignore-existing
mc mb minio/datalake --ignore-existing
mc mb minio/lakehouse --ignore-existing     # Iceberg tables for Polaris catalog
mc mb minio/dagster-pipes --ignore-existing # Dagster Pipes communication
mc mb minio/checkpoints --ignore-existing   # Spark Structured Streaming checkpoints

echo "Created buckets: warehouse, datalake, lakehouse, dagster-pipes, checkpoints"

# Create Minio policies from JSON files
mc admin policy create minio s3-rw-policy /config/s3-rw-policy.json || true

# Create Minio user for S3 data access (R/W)
mc admin user add minio "${AWS_ACCESS_KEY_ID}" "${AWS_SECRET_ACCESS_KEY}" || true
mc admin policy attach minio s3-rw-policy --user "${AWS_ACCESS_KEY_ID}" || true

# List all buckets for verification
echo "Available buckets:"
mc ls minio

echo "MinIO setup complete!"
