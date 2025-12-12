#!/usr/bin/env python3
"""Bootstrap Polaris catalog for the Streamify lakehouse.

This script:
1. Authenticates to Polaris using admin credentials
2. Creates the 'lakehouse' catalog backed by MinIO
3. Creates a 'streamify' principal with full access
4. Outputs Spark configuration for connecting to Polaris

Environment variables (set by docker-compose):
- POLARIS_HOST: Polaris hostname (default: polaris)
- MINIO_HOST: MinIO hostname (default: minio)
"""

import json
import os
import time

import requests

# Configuration from environment
POLARIS_HOST = os.getenv("POLARIS_HOST", "polaris")
MINIO_HOST = os.getenv("MINIO_HOST", "minio")

# Admin bootstrap credentials (from POLARIS_BOOTSTRAP_CREDENTIALS env var)
ADMIN_CLIENT_ID = os.getenv("POLARIS_BOOTSTRAP_CREDENTIALS_CLIENT_ID", "admin")
ADMIN_CLIENT_SECRET = os.getenv("POLARIS_BOOTSTRAP_CREDENTIALS_CLIENT_SECRET", "password")

# Catalog configuration
CATALOG_NAME = os.getenv("POLARIS_CATALOG", "lakehouse")
NAMESPACE_NAME = os.getenv("POLARIS_NAMESPACE", "streamify")

# Principal to create
PRINCIPAL_NAME = os.getenv("POLARIS_PRINCIPAL_NAME", "streamify_user")

# Polaris endpoints
POLARIS_MANAGEMENT = f"http://{POLARIS_HOST}:8181/api/management/v1"
POLARIS_CATALOG_URI = f"http://{POLARIS_HOST}:8181/api/catalog"
AUTH_URL = f"http://{POLARIS_HOST}:8181/api/catalog/v1/oauth/tokens"



def authenticate(max_retries: int = 30, retry_interval: int = 3) -> str:
    """Authenticate to Polaris via OAuth2 client credentials flow."""
    payload = {
        "grant_type": "client_credentials",
        "client_id": ADMIN_CLIENT_ID,
        "client_secret": ADMIN_CLIENT_SECRET,
        "scope": "PRINCIPAL_ROLE:ALL",
    }

    for attempt in range(max_retries):
        try:
            response = requests.post(AUTH_URL, data=payload, timeout=10)
            if response.status_code == 200:
                print(f"[OK] Authenticated as admin (attempt {attempt + 1})")
                return response.json()["access_token"]
            print(
                f"[WAIT] Auth returned {response.status_code}, "
                f"retrying in {retry_interval}s..."
            )
        except requests.exceptions.ConnectionError:
            print(
                f"[WAIT] Polaris not ready (attempt {attempt + 1}/{max_retries}), "
                f"retrying in {retry_interval}s..."
            )
        except Exception as e:
            print(f"[ERROR] Unexpected error: {e}")

        time.sleep(retry_interval)

    raise Exception("Failed to authenticate to Polaris after max retries")


def ensure_catalog(headers: dict) -> None:
    """Create the lakehouse catalog if it doesn't exist."""
    # Check if catalog exists
    response = requests.get(
        f"{POLARIS_MANAGEMENT}/catalogs/{CATALOG_NAME}",
        headers=headers,
        timeout=10,
    )

    if response.status_code == 200:
        print(f"[OK] Catalog '{CATALOG_NAME}' already exists")
        return

    # Create catalog
    catalog_config = {
        "catalog": {
            "name": CATALOG_NAME,
            "type": "INTERNAL",
            "properties": {
                "default-base-location": f"s3://{CATALOG_NAME}",
            },
            "storageConfigInfo": {
                "storageType": "S3",
                "allowedLocations": [f"s3://{CATALOG_NAME}/*"],
                "region": "us-east-1",
                "endpoint": f"http://{MINIO_HOST}:9000",
                "pathStyleAccess": True,
                "stsUnavailable": True,
            },
        }
    }

    response = requests.post(
        f"{POLARIS_MANAGEMENT}/catalogs",
        headers=headers,
        data=json.dumps(catalog_config),
        timeout=10,
    )

    if response.status_code in [200, 201]:
        print(f"[OK] Catalog '{CATALOG_NAME}' created")
    elif response.status_code == 409:
        print(f"[OK] Catalog '{CATALOG_NAME}' already exists (409)")
    else:
        print(f"[ERROR] Failed to create catalog: {response.status_code}")
        print(f"        Response: {response.text}")
        raise Exception(f"Failed to create catalog: {response.text}")


def ensure_principal(headers: dict) -> dict:
    """Create the streamify principal and return credentials."""
    # Check if principal exists
    response = requests.get(
        f"{POLARIS_MANAGEMENT}/principals/{PRINCIPAL_NAME}",
        headers=headers,
        timeout=10,
    )

    if response.status_code == 200:
        print(f"[OK] Principal '{PRINCIPAL_NAME}' already exists, rotating credentials")
        # Rotate credentials for existing principal
        rotate_response = requests.post(
            f"{POLARIS_MANAGEMENT}/principals/{PRINCIPAL_NAME}/rotate",
            headers=headers,
            timeout=10,
        )
        if rotate_response.status_code == 200:
            creds = rotate_response.json()["credentials"]
            print(f"[OK] Rotated credentials for '{PRINCIPAL_NAME}'")
            return creds
        else:
            raise Exception(f"Failed to rotate credentials: {rotate_response.text}")

    # Create new principal
    principal_config = {
        "principal": {
            "name": PRINCIPAL_NAME,
            "properties": {"purpose": "streamify-lakehouse"},
        },
        "credentialRotationRequired": False,
    }

    response = requests.post(
        f"{POLARIS_MANAGEMENT}/principals",
        headers=headers,
        data=json.dumps(principal_config),
        timeout=10,
    )

    if response.status_code == 201:
        data = response.json()
        creds = data["credentials"]
        print(f"[OK] Created principal '{PRINCIPAL_NAME}'")
        return creds
    elif response.status_code == 409:
        print(f"[OK] Principal '{PRINCIPAL_NAME}' already exists (409)")
        # Try to rotate credentials
        rotate_response = requests.post(
            f"{POLARIS_MANAGEMENT}/principals/{PRINCIPAL_NAME}/rotate",
            headers=headers,
            timeout=10,
        )
        if rotate_response.status_code == 200:
            return rotate_response.json()["credentials"]
        raise Exception(f"Failed to rotate credentials: {rotate_response.text}")
    else:
        raise Exception(f"Failed to create principal: {response.text}")


def setup_roles_and_grants(headers: dict) -> None:
    """Create roles and grant permissions to the principal."""
    principal_role = f"{PRINCIPAL_NAME}_role"
    catalog_role = f"{CATALOG_NAME}_role"

    # Create principal role
    role_body = {"principalRole": {"name": principal_role}}
    response = requests.post(
        f"{POLARIS_MANAGEMENT}/principal-roles",
        headers=headers,
        data=json.dumps(role_body),
        timeout=10,
    )
    if response.status_code in [200, 201, 409]:
        print(f"[OK] Principal role '{principal_role}' ready")
    else:
        print(f"[WARN] Principal role creation: {response.status_code}")

    # Assign principal role to principal
    response = requests.put(
        f"{POLARIS_MANAGEMENT}/principals/{PRINCIPAL_NAME}/principal-roles",
        headers=headers,
        data=json.dumps(role_body),
        timeout=10,
    )
    if response.status_code in [200, 201, 204]:
        print(f"[OK] Assigned '{principal_role}' to '{PRINCIPAL_NAME}'")

    # Create catalog role
    catalog_role_body = {"catalogRole": {"name": catalog_role}}
    response = requests.post(
        f"{POLARIS_MANAGEMENT}/catalogs/{CATALOG_NAME}/catalog-roles",
        headers=headers,
        data=json.dumps(catalog_role_body),
        timeout=10,
    )
    if response.status_code in [200, 201, 409]:
        print(f"[OK] Catalog role '{catalog_role}' ready")

    # Assign catalog role to principal role
    response = requests.put(
        f"{POLARIS_MANAGEMENT}/principal-roles/{principal_role}/catalog-roles/{CATALOG_NAME}",
        headers=headers,
        data=json.dumps(catalog_role_body),
        timeout=10,
    )
    if response.status_code in [200, 201, 204]:
        print(f"[OK] Assigned '{catalog_role}' to '{principal_role}'")

    # Grant CATALOG_MANAGE_CONTENT privilege
    grant_body = {"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}
    response = requests.put(
        f"{POLARIS_MANAGEMENT}/catalogs/{CATALOG_NAME}/catalog-roles/{catalog_role}/grants",
        headers=headers,
        data=json.dumps(grant_body),
        timeout=10,
    )
    if response.status_code in [200, 201, 204]:
        print(f"[OK] Granted CATALOG_MANAGE_CONTENT on '{CATALOG_NAME}'")


def print_spark_config(credentials: dict) -> None:
    """Print Spark configuration for connecting to Polaris."""
    client_id = credentials["clientId"]
    client_secret = credentials["clientSecret"]

    print("\n" + "=" * 70)
    print("POLARIS SETUP COMPLETE")
    print("=" * 70)

    print(f"\nCatalog: {CATALOG_NAME}")
    print(f"Principal: {PRINCIPAL_NAME}")
    print(f"Client ID: {client_id}")
    print(f"Client Secret: {client_secret}")

    print("\n" + "-" * 70)
    print("SPARK CONFIGURATION")
    print("-" * 70)

    spark_config = f"""
         # Add to spark-submit or SparkSession.builder:

        spark = (SparkSession.builder
            .appName("StreamifyLakehouse")
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1,"
                    "org.apache.iceberg:iceberg-aws-bundle:1.7.1")
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lakehouse.type", "rest")
            .config("spark.sql.catalog.lakehouse.uri", "http://polaris:8181/api/catalog")
            .config("spark.sql.catalog.lakehouse.warehouse", "{CATALOG_NAME}")
            .config("spark.sql.catalog.lakehouse.credential", "{client_id}:{client_secret}")
            .config("spark.sql.catalog.lakehouse.scope", "PRINCIPAL_ROLE:ALL")
            .config("spark.sql.catalog.lakehouse.header.X-Iceberg-Access-Delegation",
                    "vended-credentials")
            .config("spark.sql.catalog.lakehouse.token-refresh-enabled", "true")
            .config("spark.sql.defaultCatalog", "lakehouse")
            .getOrCreate())

        # Create namespace:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.{NAMESPACE_NAME}")
    """
    print(spark_config)

    # Write credentials to a file for other services to use
    creds_file = "/config/polaris_credentials.env"
    try:
        with open(creds_file, "w") as f:
            f.write(f"POLARIS_CLIENT_ID={client_id}\n")
            f.write(f"POLARIS_CLIENT_SECRET={client_secret}\n")
            f.write(f"POLARIS_CATALOG={CATALOG_NAME}\n")
            f.write(f"POLARIS_NAMESPACE={NAMESPACE_NAME}\n")
            f.write("POLARIS_URI=http://polaris:8181/api/catalog\n")
        print(f"\n[OK] Credentials written to {creds_file}")
    except Exception as e:
        print(f"\n[WARN] Could not write credentials file: {e}")


def main() -> None:
    """Main entry point."""
    print("\n" + "=" * 70)
    print("POLARIS LAKEHOUSE SETUP")
    print("=" * 70 + "\n")

    # Authenticate
    token = authenticate()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # Setup catalog
    ensure_catalog(headers)

    # Setup principal
    credentials = ensure_principal(headers)

    # Setup roles and grants
    setup_roles_and_grants(headers)

    # Print configuration
    print_spark_config(credentials)

    print("\n[DONE] Polaris setup complete!\n")


if __name__ == "__main__":
    main()
