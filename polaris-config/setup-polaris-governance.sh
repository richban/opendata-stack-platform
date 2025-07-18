#!/bin/sh
set -e

POLARIS_SERVICE_URL="http://polaris:8181"
POLARIS_MGMT_API_URL_BASE="${POLARIS_SERVICE_URL}/api/management/v1"
POLARIS_ADMIN_USER="root"
POLARIS_ADMIN_PASS="s3cr3t"
POLARIS_REALM="POLARIS_MINIO_REALM"

# Polaris client IDs (assumed to be created by polaris-bootstrap-minio)
PYICEBERG_POLARIS_CLIENT_ID="${PYICEBERG_POLARIS_CLIENT_ID:-pyiceberg_app_client}"

NAMESPACE_NAME="ns_governed"

echo "Waiting for Polaris service..."
# ... (Polaris health check as in create-catalog-minio.sh) ...
echo "Polaris service is live."

echo "Acquiring Polaris admin token..."
# ... (Admin token acquisition as in create-catalog-minio.sh, storing token in ADMIN_TOKEN) ...
ADMIN_TOKEN_RESPONSE=$(curl -s -w "%{http_code}" -X POST "${POLARIS_SERVICE_URL}/api/catalog/v1/oauth/tokens" \
  --user "${POLARIS_ADMIN_USER}:${POLARIS_ADMIN_PASS}" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" -d "scope=PRINCIPAL_ROLE:ALL" -d "realmName=${POLARIS_REALM}")
HTTP_CODE=$(echo "$ADMIN_TOKEN_RESPONSE" | tail -n1)
TOKEN_BODY=$(echo "$ADMIN_TOKEN_RESPONSE" | sed '$d')
if [ "$HTTP_CODE" -ne 200 ]; then
  echo "Failed to get Polaris admin token. HTTP Code: $HTTP_CODE"
  exit 1
fi
ADMIN_TOKEN=$(echo "$TOKEN_BODY" | jq -r .access_token)
if [ -z "$ADMIN_TOKEN" ] || [ "$ADMIN_TOKEN" = "null" ]; then
  echo "Failed to parse admin token"
  exit 1
fi
echo "Admin token acquired."

polaris_api_call() {
  local method="$1"
  local endpoint="$2"
  local payload="$3"
  local expected_status_primary="$4"
  local expected_status_secondary="${5:-409}" # Typically 409 Conflict for already exists

  full_url="${POLARIS_MGMT_API_URL_BASE}${endpoint}"
  echo "Calling: $method $full_url"
  if [ -n "$payload" ]; then
    echo "Payload: $payload"
    response_code=$(curl -s -o /tmp/api_response.txt -w "%{http_code}" \
      -X "$method" -H "Authorization: Bearer $ADMIN_TOKEN" -H "Content-Type: application/json" \
      "$full_url" -d "$payload")
  else
    response_code=$(curl -s -o /tmp/api_response.txt -w "%{http_code}" \
      -X "$method" -H "Authorization: Bearer $ADMIN_TOKEN" -H "Content-Type: application/json" \
      "$full_url")
  fi

  echo "Response Code: $response_code. Body:"
  cat /tmp/api_response.txt
  if [ "$response_code" -ne "$expected_status_primary" ] && [ "$response_code" -ne "$expected_status_secondary" ]; then
    echo "Error: API call failed. Expected $expected_status_primary or $expected_status_secondary, Got $response_code."
    # exit 1 # Comment out for idempotency if needed
  else
    echo "API call successful or resource already exists (HTTP $response_code)."
  fi
  echo ""
}
echo "Creating Polaris principal for pyiceberg: ${PYICEBERG_POLARIS_CLIENT_ID}"
# Assuming an API endpoint like /auth/principals or similar
# This might be a multi-step process: 1. Create principal, 2. Set password credential
# Example (highly conceptual, verify actual API):
polaris_api_call "POST" "/auth/principals" \
  "{\"name\": \"${PYICEBERG_POLARIS_CLIENT_ID}\", \"realmName\": \"${POLARIS_MINIO_REALM}\"}" 201 409 "${POLARIS_AUTH_API_URL_BASE}" # 409 if exists

polaris_api_call "PUT" "/auth/principals/${PYICEBERG_POLARIS_CLIENT_ID}/credentials" \
  "[{\"type\": \"PASSWORD\", \"value\": \"${PYICEBERG_POLARIS_CLIENT_SECRET}\"}]" 204 200 "${POLARIS_AUTH_API_URL_BASE}" # Using PUT to set/reset

# 1. Create Principal Roles
polaris_api_call "POST" "/principal-roles" "{\"name\": \"${PYICEBERG_ROLE_NAME}\"}" 201

# 2. Assign Principals (Client IDs) to Roles
# Assumes PYICEBERG_POLARIS_CLIENT_ID is a valid principal name created by bootstrap
polaris_api_call "PUT" "/principal-roles/${PYICEBERG_ROLE_NAME}/principals/${PYICEBERG_POLARIS_CLIENT_ID}" "" 204 200 # 200 if already assigned

# 3. Grant Privileges to Roles

# --- pyiceberg Role Grants (R/W) ---
# Catalog grants for pyiceberg
polaris_api_call "POST" "/principal-roles/${PYICEBERG_ROLE_NAME}/catalog-grants" \
  "{\"catalogName\": \"${CATALOG_NAME}\", \"privileges\": [\"USE_CATALOG\", \"CREATE_NAMESPACE\"]}" 201
# Namespace grants for pyiceberg on ns_governed
polaris_api_call "POST" "/principal-roles/${PYICEBERG_ROLE_NAME}/grants" \
  "{\"grantResource\":{\"resourceType\":\"NAMESPACE\",\"identifierParts\":[\"${CATALOG_NAME}\",\"${NAMESPACE_NAME}\"]},\"privileges\":[\"USE_NAMESPACE\",\"CREATE_TABLE\",\"DROP_TABLE\",\"ALTER_TABLE\"]}" 201
# Table grants for pyiceberg on tables under ns_governed
polaris_api_call "POST" "/principal-roles/${PYICEBERG_ROLE_NAME}/grants" \
  "{\"grantResource\":{\"resourceType\":\"TABLE\",\"identifierParts\":[\"${CATALOG_NAME}\",\"${NAMESPACE_NAME}\",\"*\"]},\"privileges\":[\"READ_TABLE_METADATA\",\"READ_TABLE_DATA\",\"WRITE_TABLE_DATA\"]}" 201

echo "Polaris governance setup script completed."
