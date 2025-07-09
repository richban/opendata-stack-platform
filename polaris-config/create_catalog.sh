#!/bin/bash

# Exit on any error
set -e

# Set the path to the Polaris binary
POLARIS_BIN="${POLARIS_BIN:-/Users/melchior/Developer/polaris/polaris}"

# Load environment variables from .env file
source ./dagster-workspace/team_data_platform/data_platform/.env

# Function to run a Polaris command and print its output
run_polaris_command() {
  echo "Running: $@"
  OUTPUT=$("$@")
  echo "Output: $OUTPUT"
  echo "----------------------------------------"
}

# Create catalog
echo "Creating catalog..."
run_polaris_command ${POLARIS_BIN} \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  catalogs create \
  --storage-type s3 \
  --default-base-location ${DEFAULT_BASE_LOCATION} \
  --property warehouse=s3://datalake/pyiceberg_catalog \
  --property s3.endpoint=http://localhost:9000 \
  --property s3.access-key-id=polaris_s3_user \
  --property s3.secret-access-key=polaris_s3_password_val \
  --property s3.path-style-access=true \
  --property s3.region=us-east-1 \
  --property s3.allow-http=true \
  --storage-type S3 \
  --allowed-location s3://datalake/pyiceberg_catalog \
  ${CATALOG_NAME}

# Create principal
echo "Creating principal..."
run_polaris_command ${POLARIS_BIN} \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  principals \
  create \
  ${POLARIS_USER}

# Create principal role
echo "Creating principal role..."
run_polaris_command ${POLARIS_BIN} \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  principal-roles \
  create \
  ${POLARIS_USER_ROLE}

# Create catalog role
echo "Creating catalog role..."
run_polaris_command ${POLARIS_BIN} \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  catalog-roles \
  create \
  --catalog ${CATALOG_NAME} \
  ${POLARIS_CATALOG_ROLE}

# Grant principal role
echo "Granting principal role..."
run_polaris_command ${POLARIS_BIN} \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  principal-roles \
  grant \
  --principal ${POLARIS_USER} \
  ${POLARIS_USER_ROLE}

# Grant catalog role
echo "Granting catalog role..."
run_polaris_command ${POLARIS_BIN} \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  catalog-roles \
  grant \
  --catalog ${CATALOG_NAME} \
  --principal-role ${POLARIS_USER_ROLE} \
  ${POLARIS_CATALOG_ROLE}

# Grant privileges
echo "Granting privileges..."
run_polaris_command ${POLARIS_BIN} \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  privileges \
  catalog \
  grant \
  --catalog ${CATALOG_NAME} \
  --catalog-role ${POLARIS_CATALOG_ROLE} \
  CATALOG_MANAGE_CONTENT

echo "All operations completed successfully!"
