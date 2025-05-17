#!/bin/sh
set -e

mc alias set minio http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"
mc mb minio/warehouse --ignore-existing
mc mb minio/datalake --ignore-existing

# Create Minio policies from JSON files
mc admin policy create minio polaris-s3-rw-policy /config/polaris-s3-rw-policy.json

# Create Minio user for Polaris Service (R/W)
mc admin user add minio "${POLARIS_S3_USER}" "${POLARIS_S3_PASSWORD}"
mc admin policy attach minio polaris-s3-rw-policy --user "${POLARIS_S3_USER}"

echo "Minio setup complete: users (polaris_s3_user and policies configured."
