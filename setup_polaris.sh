#!/bin/bash

# Exit on any error
set -e

# Load environment variables from .env file
source ./dagster-workspace/team_data_platform/data_platform/.env

# Set variables for user and roles
POLARIS_USER="taxi_user"
POLARIS_USER_ROLE="taxi_user_role"
POLARIS_CATALOG_ROLE="taxi_catalog_role"

PRINCIPAL_TOKEN=$(curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens -d "grant_type=client_credentials&client_id=${CLIENT_ID}&client_secret=${CLIENT_SECRET}&scope=PRINCIPAL_ROLE:ALL" | jq -r '.access_token')

# Set the Polaris server URL
POLARIS_URL="http://localhost:8181"

# Set the catalog name
CATALOG_NAME="taxi_catalog"

# Create the catalog
echo "Creating catalog..."
curl -i -X POST \
  -H "Authorization: Bearer $PRINCIPAL_TOKEN" \
  -H 'Accept: application/json' \
  -H 'Content-Type: application/json' \
  $POLARIS_URL/api/management/v1/catalogs \
  -d '{
    "catalog": {
      "name": "'$CATALOG_NAME'",
      "type": "INTERNAL",
      "readOnly": false,
      "properties": {
        "default-base-location": "s3://iceberg",
        "s3.endpoint": "'$AWS_ENDPOINT_URL'",
        "s3.access-key-id": "'$AWS_ACCESS_KEY_ID'",
        "s3.secret-access-key": "'$AWS_SECRET_ACCESS_KEY'",
        "s3.path-style-access": "true",
        "s3.region": "'$AWS_REGION'",
        "s3.allow-http": "true"
      },
      "storageConfigInfo": {
        "storageType": "S3",
        "allowedLocations": [
          "s3://iceberg"
        ],
        "credentials": {
          "type": "ACCESS_KEY",
          "accessKeyId": "'$AWS_ACCESS_KEY_ID'",
          "secretAccessKey": "'$AWS_SECRET_ACCESS_KEY'"
        },
        "roleArn": "'$AWS_ROLE_ARN'"
      }
    }
  }'

# Create a principal and capture the response
PRINCIPAL_RESPONSE=$(curl -s -X POST "$POLARIS_URL/api/management/v1/principals" \
  -H "Authorization: Bearer $PRINCIPAL_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "'$POLARIS_USER'", "type": "user"}')

echo $PRINCIPAL_RESPONSE

# Create a principal role
curl -X POST "$POLARIS_URL/api/management/v1/principal-roles" \
  -H "Authorization: Bearer $PRINCIPAL_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"principalRole": {"name": "'$POLARIS_USER_ROLE'"}}'

# Assign the principal role to the polaris_user principal
echo "Assigning principal role to $POLARIS_USER..."
curl -X PUT "$POLARIS_URL/api/management/v1/principals/$POLARIS_USER/principal-roles" \
  -H "Authorization: Bearer $PRINCIPAL_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"principalRole": {"name": "'$POLARIS_USER_ROLE'"}}'

# Create a catalog role
echo "Creating catalog role..."
curl -X POST "$POLARIS_URL/api/management/v1/catalogs/$CATALOG_NAME/catalog-roles" \
  -H "Authorization: Bearer $PRINCIPAL_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"catalogRole": {"name": "'$POLARIS_CATALOG_ROLE'"}}'

# Assign the catalog role to the principal role
echo "Assigning catalog role to principal role..."
curl -X PUT "$POLARIS_URL/api/management/v1/principal-roles/$POLARIS_USER_ROLE/catalog-roles/$CATALOG_NAME" \
  -H "Authorization: Bearer $PRINCIPAL_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"catalogRole": {"name": "'$POLARIS_CATALOG_ROLE'"}}'

# Grant privileges to the catalog role
echo "Granting privileges to catalog role..."
curl -X PUT "$POLARIS_URL/api/management/v1/catalogs/$CATALOG_NAME/catalog-roles/$POLARIS_CATALOG_ROLE/grants" \
  -H "Authorization: Bearer $PRINCIPAL_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}'

echo "Setup complete!"
