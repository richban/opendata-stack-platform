#!/bin/sh
set -e

mc alias set minio http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"
mc mb minio/warehouse --ignore-existing
mc mb minio/datalake --ignore-existing

# Create Minio policies from JSON files
mc admin policy create minio s3-rw-policy /config/s3-rw-policy.json

# Create Minio user for S3 data access (R/W)
mc admin user add minio "${AWS_ACCESS_KEY_ID}" "${AWS_SECRET_ACCESS_KEY}"
mc admin policy attach minio s3-rw-policy --user "${AWS_ACCESS_KEY_ID}"

echo "Minio setup complete: users (polaris_s3_user and policies configured."
