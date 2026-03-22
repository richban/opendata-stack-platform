from contextlib import contextmanager

from dagster import EnvVar
from dagster_aws.s3 import S3Resource
from dagster_duckdb import DuckDBResource
from dagster_snowflake import SnowflakeResource

from data_platform.utils.environment_helpers import get_environment


class ExtendedDuckDBResource(DuckDBResource):
    """Extended DuckDB resource that pre-installs and loads extensions."""

    def _initialize_extensions(self, conn):
        """Initialize DuckDB extensions on a connection."""
        extensions = [
            "httpfs",  # For S3 access
            "aws",  # For AWS credentials
            "ducklake",  # For DuckLake format
            "spatial",  # For spatial functions
        ]

        for extension in extensions:
            try:
                conn.execute(f"INSTALL {extension}")
                conn.execute(f"LOAD {extension}")
            except Exception as e:
                # Log warning but continue - some extensions might not be available
                print(f"Warning: Could not load extension '{extension}': {e}")

    @contextmanager
    def get_connection(self):
        """Get a DuckDB connection with all extensions pre-loaded."""
        with super().get_connection() as conn:
            self._initialize_extensions(conn)
            yield conn


# Create environment-aware S3 and DuckDB resources
def _get_duckdb_s3_config():
    """Get S3 configuration for DuckDB based on environment."""
    import os

    config = {
        "s3_access_key_id": EnvVar("AWS_ACCESS_KEY_ID"),
        "s3_secret_access_key": EnvVar("AWS_SECRET_ACCESS_KEY"),
        "s3_region": EnvVar("AWS_REGION"),
    }

    # Only set endpoint for local development (MinIO)
    if get_environment() == "dev":
        endpoint_url = os.getenv("AWS_ENDPOINT_URL")
        if endpoint_url:
            config.update(
                {
                    "s3_endpoint": endpoint_url.replace("http://", "").replace(
                        "https://", ""
                    ),
                    "s3_use_ssl": False,
                    "s3_url_style": "path",
                }
            )

    return config


def _create_s3_resource():
    """Create S3 resource with environment-aware endpoint configuration."""
    config = {
        "aws_access_key_id": EnvVar("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": EnvVar("AWS_SECRET_ACCESS_KEY"),
    }

    # Only set endpoint for local development (MinIO)
    if get_environment() == "dev":
        config["endpoint_url"] = EnvVar("AWS_ENDPOINT_URL")

    return S3Resource(**config)


s3_resource = _create_s3_resource()

duckdb_resource = ExtendedDuckDBResource(
    database=EnvVar("DUCKDB_DATABASE"),
    connection_config=_get_duckdb_s3_config(),
)

# Snowflake resource for production
snowflake_resource = SnowflakeResource(
    account=EnvVar("SNOWFLAKE_ACCOUNT"),
    user=EnvVar("DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME"),
    password=EnvVar("DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD"),
    database=EnvVar("SNOWFLAKE_DATABASE"),
    warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),
    role=EnvVar("DESTINATION__SNOWFLAKE__CREDENTIALS__ROLE"),
)
