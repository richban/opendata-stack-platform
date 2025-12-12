#!/usr/bin/env python3
"""Test Snowflake connection using the same credentials as DLT"""

import os

import snowflake.connector


def test_snowflake_connection():
    """Test Snowflake connection with DLT credentials"""

    # Get credentials from environment (same as DLT uses)
    # credentials = {
    #     "user": os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME"),
    #     "password": os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD"),
    #     "account": os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__HOST"),
    #     "warehouse": os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE"),
    #     "database": os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE"),
    #     "schema": os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__SCHEMA"),
    #     "role": os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__ROLE"),
    # }
    credentials = {
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__SCHEMA"),
        "role": os.getenv("SNOWFLAKE_ROLE_SQLMESH"),
    }

    print("Testing Snowflake connection with credentials:")
    for key, value in credentials.items():
        if key == "password":
            print(f"  {key}: {'*' * len(value) if value else 'None'}")
        else:
            print(f"  {key}: {value}")
    print()

    try:
        # Test connection
        print("Connecting to Snowflake...")
        conn = snowflake.connector.connect(**credentials)

        cursor = conn.cursor()

        # Test basic queries
        print("✅ Connection successful!")

        # Set context explicitly if not set automatically
        database = credentials["database"]
        warehouse = credentials["warehouse"]
        schema = credentials["schema"]

        if database:
            cursor.execute(f"USE DATABASE {database}")
            print(f"✅ Set database context: {database}")

        if warehouse:
            cursor.execute(f"USE WAREHOUSE {warehouse}")
            print(f"✅ Set warehouse context: {warehouse}")

        # List available schemas first
        cursor.execute("SHOW SCHEMAS IN DATABASE NYC_DATABASE")
        schemas = cursor.fetchall()
        print(f"📋 Available schemas in NYC_DATABASE:")
        for schema_info in schemas:
            print(f"  - {schema_info[1]}")  # schema name is typically in column 1
        print()

        if schema:
            try:
                cursor.execute(f"USE SCHEMA {schema}")
                print(f"✅ Set schema context: {schema}")
            except Exception as e:
                print(f"⚠️ Could not set schema {schema}: {e}")
                # Try with database.schema format
                try:
                    cursor.execute(f"USE SCHEMA {database}.{schema}")
                    print(f"✅ Set schema context: {database}.{schema}")
                except Exception as e2:
                    print(f"❌ Failed to set schema with database prefix: {e2}")

        # Check current context after setting
        cursor.execute(
            "SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE(), CURRENT_WAREHOUSE()"
        )
        result = cursor.fetchone()
        print("📍 Current context:")
        print(f"  Database: {result[0]}")
        print(f"  Schema: {result[1]}")
        print(f"  Role: {result[2]}")
        print(f"  Warehouse: {result[3]}")
        print()

        # Test if we can query without USE DATABASE
        try:
            cursor.execute("SELECT 1 as test_column")
            result = cursor.fetchone()
            print(f"✅ Can execute SELECT without USE DATABASE: {result[0]}")
        except Exception as e:
            print(f"❌ Cannot execute SELECT without USE DATABASE: {e}")

            # Try with explicit USE statements
            try:
                cursor.execute("USE DATABASE NYC_DATABASE")
                cursor.execute("USE SCHEMA PUBLIC")
                cursor.execute("SELECT 1 as test_column")
                result = cursor.fetchone()
                print(f"✅ Can execute SELECT after USE DATABASE: {result[0]}")
            except Exception as e2:
                print(f"❌ Still fails after USE DATABASE: {e2}")

        # Test permissions
        try:
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            print(f"✅ Can list tables: {len(tables)} tables found")
        except Exception as e:
            print(f"❌ Cannot list tables: {e}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Connection failed: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_snowflake_connection()
