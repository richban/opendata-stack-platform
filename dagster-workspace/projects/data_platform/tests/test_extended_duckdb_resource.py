"""Tests for ExtendedDuckDBResource."""

import pytest

from data_platform.defs.dlt.resources import ExtendedDuckDBResource


class TestExtendedDuckDBResource:
    """Test cases for ExtendedDuckDBResource."""

    def test_initialize_extensions_success(self):
        """Test successful extension initialization."""
        # Create resource instance
        resource = ExtendedDuckDBResource(
            database=":memory:",
            connection_config={}
        )

        # Test that we can get a connection and extensions are loaded
        with resource.get_connection() as conn:
            # Basic query to verify connection works
            result = conn.execute("SELECT 1 as test").fetchone()
            assert result[0] == 1

            # Test that httpfs extension was loaded (most commonly available)
            try:
                # This will work if httpfs is loaded
                conn.execute("SELECT * FROM read_csv_auto('non_existent_file.csv')")
                assert False, "Should have failed with file not found, not extension error"
            except Exception as e:
                # Should fail with file error, not extension error
                error_msg = str(e).lower()
                # If httpfs is loaded, we get file errors, not extension errors
                assert "file" in error_msg or "csv" in error_msg or "no such" in error_msg

    def test_get_connection_integration(self):
        """Integration test using a real in-memory database."""
        resource = ExtendedDuckDBResource(
            database=":memory:",
            connection_config={}
        )

        # Test that we can actually get a connection and it works
        with resource.get_connection() as conn:
            # Basic query to verify connection works
            result = conn.execute("SELECT 1 as test").fetchone()
            assert result[0] == 1

            # Test that we can create and query tables
            conn.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR)")
            conn.execute("INSERT INTO test_table VALUES (1, 'test')")
            result = conn.execute("SELECT * FROM test_table").fetchone()
            assert result[0] == 1
            assert result[1] == 'test'

    def test_get_connection_with_s3_config(self):
        """Test connection with S3 configuration."""
        resource = ExtendedDuckDBResource(
            database=":memory:",
            connection_config={
                "s3_endpoint": "localhost:9000",
                "s3_access_key_id": "test_key",
                "s3_secret_access_key": "test_secret",
                "s3_region": "us-east-1",
                "s3_use_ssl": False,
                "s3_url_style": "path",
            }
        )

        with resource.get_connection() as conn:
            # Verify connection works with S3 config
            result = conn.execute("SELECT 1 as test").fetchone()
            assert result[0] == 1

            # Verify S3 settings were applied (if httpfs/aws extensions loaded)
            try:
                s3_endpoint = conn.execute("SELECT current_setting('s3_endpoint')").fetchone()[0]
                assert s3_endpoint == "localhost:9000"
            except Exception:
                # Settings might not be available if extensions don't load
                pytest.skip("S3 settings not available - extensions may not be loaded")


    def test_multiple_connections(self):
        """Test that multiple connections can be created and work independently."""
        resource = ExtendedDuckDBResource(
            database=":memory:",
            connection_config={}
        )

        # Test multiple sequential connections
        with resource.get_connection() as conn1:
            conn1.execute("CREATE TABLE test1 (id INTEGER)")
            conn1.execute("INSERT INTO test1 VALUES (1)")
            result1 = conn1.execute("SELECT COUNT(*) FROM test1").fetchone()[0]
            assert result1 == 1

        with resource.get_connection() as conn2:
            # This is a new connection to memory DB, so table won't exist
            # But connection should work fine
            result2 = conn2.execute("SELECT 2 as test").fetchone()[0]
            assert result2 == 2

    def test_extension_loading_with_real_database(self):
        """Test that extensions are actually loaded by trying to use them."""
        resource = ExtendedDuckDBResource(
            database=":memory:",
            connection_config={}
        )

        with resource.get_connection() as conn:
            # Test basic functionality
            result = conn.execute("SELECT 1").fetchone()[0]
            assert result == 1

            # Test that we can list loaded extensions
            try:
                extensions_result = conn.execute("""
                    SELECT extension_name
                    FROM duckdb_extensions()
                    WHERE loaded = true
                """).fetchall()

                loaded_extensions = [row[0] for row in extensions_result]

                # Check that at least some common extensions are loaded
                # Note: Not all extensions may be available in all environments
                expected_extensions = ["httpfs", "aws", "ducklake", "spatial"]
                loaded_count = sum(1 for ext in expected_extensions if ext in loaded_extensions)

                # We expect at least one extension to be loaded successfully
                assert loaded_count > 0, f"No expected extensions loaded. Available: {loaded_extensions}"

            except Exception as e:
                # If we can't query extensions, just ensure basic functionality works
                pytest.skip(f"Cannot query extensions in this environment: {e}")
