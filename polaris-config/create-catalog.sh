#!/bin/sh
set -e

POLARIS_SERVICE_URL="http://polaris:8181"
POLARIS_MGMT_API_URL="${POLARIS_SERVICE_URL}/api/management/v1/catalogs"
POLARIS_TOKEN_URL="${POLARIS_SERVICE_URL}/api/catalog/v1/oauth/tokens"
POLARIS_ADMIN_USER="root"
POLARIS_ADMIN_PASS="s3cr3t"
POLARIS_REALM="POLARIS_MINIO_REALM"

CATALOG_WAREHOUSE_PATH="s3://${DEFAULT_BUCKET_NAME}/${CATALOG_NAME}"

S3_ACCESS_KEY="${POLARIS_S3_USER}" # Polaris service's S3 user
S3_SECRET_KEY="${POLARIS_S3_PASSWORD}"
S3_ENDPOINT="http://minio:9000"

echo "Polaris service is live."

echo "Attempting to get Polaris admin token..."
echo "Polaris admin user: ${POLARIS_ADMIN_USER}"
echo "Polaris admin password: ${POLARIS_ADMIN_PASS}" # Be careful logging this in production!

# Create a temporary file to store the response body of the token request
TOKEN_RESPONSE_FILE=$(mktemp)

# Execute curl:
# -s : Silent mode, suppresses progress and error messages (not verbose info)
# -w "%{http_code}" : Prints the HTTP status code to stdout (which will be captured by HTTP_CODE variable)
# -o "$TOKEN_RESPONSE_FILE" : Writes the response body to the temporary file
# The -v flag is removed to prevent verbose output from contaminating the captured variable.
HTTP_CODE=$(curl -s -X POST "${POLARIS_TOKEN_URL}" \
  --user "${POLARIS_ADMIN_USER}:${POLARIS_ADMIN_PASS}" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" -d "scope=PRINCIPAL_ROLE:ALL" -d "realmName=${POLARIS_REALM}" \
  -o "$TOKEN_RESPONSE_FILE" \
  -w "%{http_code}") # This writes the code to stdout, which is captured by HTTP_CODE

# Read the JSON body from the temporary file
TOKEN_BODY=$(cat "$TOKEN_RESPONSE_FILE")

# Clean up the temporary file immediately
rm "$TOKEN_RESPONSE_FILE"

# Debug: Print the captured HTTP code and the raw body if you want to see them
echo "DEBUG: Token HTTP Code: $HTTP_CODE"
echo "DEBUG: Token Body (raw, first 100 chars):"
echo "$TOKEN_BODY" | head -c 100
echo "--------------------"

if [ "$HTTP_CODE" -ne 200 ]; then
  echo "Failed to get Polaris admin token. HTTP Code: $HTTP_CODE. Response:"
  echo "$TOKEN_BODY" # This will now correctly print the JSON error body if any
  exit 1
fi

# Now, TOKEN_BODY should contain pure JSON, so jq can parse it
ADMIN_TOKEN=$(echo "$TOKEN_BODY" | jq -r .access_token)

if [ -z "$ADMIN_TOKEN" ] || [ "$ADMIN_TOKEN" = "null" ]; then
  echo "Failed to parse admin token. Admin token was empty or null after parsing."
  echo "Full TOKEN_BODY was: $TOKEN_BODY" # Print the full content for debugging
  exit 1
fi
echo "Polaris admin token obtained."

CREATE_CATALOG_PAYLOAD=$(
  cat <<EOF
{
  "name": "${CATALOG_NAME}",
  "type": "INTERNAL",
  "readOnly": false
  "properties": {
    "warehouse": "${CATALOG_WAREHOUSE_PATH}",
    "storage.type": "s3",
    "s3.endpoint": "${S3_ENDPOINT}",
    "s3.access-key-id": "${S3_ACCESS_KEY}",
    "s3.secret-access-key": "${S3_SECRET_KEY}",
    "s3.path-style-access": "true",
    "client.region": "${AWS_REGION}"
  },
  "storageConfigInfo": {
    "allowedLocations": [${CATALOG_WAREHOUSE_PATH}],
  }
}
EOF
)

echo "Attempting to create/verify catalog '${CATALOG_NAME}'..."

# --- Add this line to print the payload being sent ---
echo "Payload being sent: ${CREATE_CATALOG_PAYLOAD}"
echo "--------------------"
# --------------------------------------------------

STATUS_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer ${ADMIN_TOKEN}" "${POLARIS_MGMT_API_URL}/${CATALOG_NAME}")

if [ "$STATUS_CODE" -eq 200 ]; then
  echo "Catalog '${CATALOG_NAME}' already exists. Skipping creation."
elif [ "$STATUS_CODE" -eq 404 ]; then
  echo "Catalog '${CATALOG_NAME}' not found. Attempting to create..."
  # --- Temporarily remove -s here ---
  CREATE_RESPONSE_CODE=$(curl -w "%{http_code}" -X POST "${POLARIS_MGMT_API_URL}" \
    -H "Authorization: Bearer ${ADMIN_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "${CREATE_CATALOG_PAYLOAD}" \
    -o /tmp/create_catalog_response.txt) # Still try to write to file

  if [ "$CREATE_RESPONSE_CODE" -eq 201 ] || [ "$CREATE_RESPONSE_CODE" -eq 200 ]; then
    echo "Catalog '${CATALOG_NAME}' creation request successful (HTTP ${CREATE_RESPONSE_CODE})."
  else
    echo "Failed to create catalog '${CATALOG_NAME}'. HTTP Status: ${CREATE_RESPONSE_CODE}. Response:"
    # --- Try reading from file AND printing directly from curl's stdout if file is empty ---
    if [ -s /tmp/create_catalog_response.txt ]; then
      echo "--- Response body from file ---"
      cat /tmp/create_catalog_response.txt
      echo "-------------------------------"
    else
      echo "Response file /tmp/create_catalog_response.txt is empty or does not exist. Checking direct output..."
      # Since -s is removed, the body might have been printed to stdout already.
      # We can't easily re-capture stdout here, but removing -s should make it visible in the logs.
      echo "Look for response body output above this line in the logs."
    fi
    # --------------------------------------------------------------------------------------
    exit 1
  fi
else
  echo "Unexpected status code ${STATUS_CODE} when checking for catalog '${CATALOG_NAME}'."
  curl -I -s -H "Authorization: Bearer ${ADMIN_TOKEN}" "${POLARIS_MGMT_API_URL}/${CATALOG_NAME}"
  exit 1
fi
echo "Catalog '${CATALOG_NAME}' setup script completed."
