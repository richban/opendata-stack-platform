#!/usr/bin/env python3
"""
Initialize DuckLake for SQLMesh analytics export
"""

import os

import duckdb


def init_ducklake():
    """Initialize DuckLake database and S3 connection"""

    # Get database path from environment
    db_path = os.getenv('DUCKDB_DATABASE', '../data/nyc_database.duckdb')

    # Connect to source DuckDB database
    conn = duckdb.connect(db_path)

    try:
        print("Installing and loading DuckLake extension...")
        conn.execute("INSTALL ducklake;")
        conn.execute("LOAD ducklake;")

        print("Installing and loading required extensions...")
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        conn.execute("INSTALL aws;")
        conn.execute("LOAD aws;")

        print("Setting up S3 configuration...")
        # Configure S3 settings from environment variables
        aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        aws_endpoint = os.getenv('AWS_ENDPOINT_URL', 'http://localhost:9000')
        aws_region = os.getenv('AWS_REGION', 'us-east-1')

        if aws_access_key and aws_secret_key:
            # Remove http:// prefix for DuckDB configuration
            s3_endpoint = aws_endpoint.replace('http://', '').replace('https://', '')

            conn.execute(f"""
                SET s3_region = '{aws_region}';
                SET s3_access_key_id = '{aws_access_key}';
                SET s3_secret_access_key = '{aws_secret_key}';
                SET s3_endpoint = '{s3_endpoint}';
                SET s3_use_ssl = false;
                SET s3_url_style = 'path';
            """)
            print(f"S3 configuration set for endpoint: {s3_endpoint}")
        else:
            print("Warning: AWS credentials not found in environment variables")

        print("Attaching DuckLake metadata database...")
        # Attach DuckLake metadata pointing to S3
        conn.execute("""
            ATTACH 'ducklake:analytics_metadata.ducklake' AS analytics_lake
            (DATA_PATH 's3://datalake/analytics/');
        """)

        print("Switching to analytics_lake database...")
        conn.execute("USE analytics_lake;")

        print("Creating analytics schema...")
        conn.execute("CREATE SCHEMA IF NOT EXISTS analytics;")

        print("DuckLake initialization completed successfully!")
        print("Analytics lake configured at: s3://datalake/analytics/")

        # Test the setup
        conn.execute("SELECT current_database(), current_schema();")
        result = conn.fetchone()
        if result:
            print(f"Current database: {result[0]}, schema: {result[1]}")
        else:
            print("Warning: No result returned for current_database(), current_schema()")

    except Exception as e:
        print(f"Error during initialization: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    init_ducklake()
