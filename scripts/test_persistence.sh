#!/bin/bash
# Test Polaris catalog persistence across Docker restarts

set -e

echo "==================================="
echo "Polaris Persistence Test"
echo "==================================="

# Function to get Polaris auth token
get_token() {
	curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
		-d "grant_type=client_credentials" \
		-d "client_id=admin" \
		-d "client_secret=password" \
		-d "scope=PRINCIPAL_ROLE:ALL" |
		grep -o '"access_token":"[^"]*"' | cut -d'"' -f4
}

# Function to check if namespace exists
check_namespace() {
	local token=$1
	local response=$(curl -s -w "%{http_code}" -o /tmp/ns_check.json \
		-X GET http://localhost:8181/api/catalog/v1/lakehouse/namespaces \
		-H "Authorization: Bearer $token" \
		-H "Content-Type: application/json")
	echo "$response"
}

echo ""
echo "Step 1: Starting Docker containers..."
docker-compose up -d postgres minio polaris
echo "Waiting for services to be healthy (60s)..."
sleep 10

echo ""
echo "Step 2: Creating test catalog resources..."
TOKEN=$(get_token)
echo "Got auth token: ${TOKEN:0:20}..."

# Create a test namespace
echo "Creating test namespace 'persistence_test'..."
curl -s -X POST http://localhost:8181/api/catalog/v1/lakehouse/namespaces \
	-H "Authorization: Bearer $TOKEN" \
	-H "Content-Type: application/json" \
	-d '{"namespace":["persistence_test"]}' | tee /tmp/create_ns.json
echo ""

echo ""
echo "Step 3: Verifying namespace exists before restart..."
NS_CHECK=$(check_namespace "$TOKEN")
if [ "$NS_CHECK" = "200" ]; then
	echo "✓ Namespace exists (HTTP 200)"
	cat /tmp/ns_check.json | grep persistence_test && echo "✓ persistence_test namespace found!"
else
	echo "✗ Namespace check failed (HTTP $NS_CHECK)"
	exit 1
fi

echo ""
echo "Step 4: Simulating Docker restart..."
echo "  Stopping containers..."
docker-compose stop polaris postgres
echo "  Waiting 5 seconds..."
sleep 5
echo "  Starting containers again..."
docker-compose start postgres polaris
echo "  Waiting for services to be healthy (30s)..."
sleep 10

echo ""
echo "Step 5: Checking if data persisted after restart..."
TOKEN=$(get_token)
NS_CHECK=$(check_namespace "$TOKEN")
if [ "$NS_CHECK" = "200" ]; then
	echo "✓ Catalog is accessible (HTTP 200)"
	if cat /tmp/ns_check.json | grep -q persistence_test; then
		echo "✅ SUCCESS: persistence_test namespace survived restart!"
		echo "   Catalog metadata is now persisted in PostgreSQL."
	else
		echo "✗ FAIL: persistence_test namespace not found after restart"
		echo "   Catalog was not persisted."
		exit 1
	fi
else
	echo "✗ FAIL: Cannot connect to catalog (HTTP $NS_CHECK)"
	exit 1
fi

echo ""
echo "==================================="
echo "Test Complete!"
echo "==================================="
echo ""
echo "Your Polaris catalog is now configured with PostgreSQL persistence."
echo "Catalog metadata (namespaces, tables, schemas) will survive Docker restarts."
