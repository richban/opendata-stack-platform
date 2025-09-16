from contextlib import contextmanager

from dagster import EnvVar
from dagster_duckdb import DuckDBResource


class ExtendedDuckDBResource(DuckDBResource):
    """Extended DuckDB resource that pre-installs and loads extensions."""

    def _initialize_extensions(self, conn):
        """Initialize DuckDB extensions on a connection."""
        extensions = [
            "httpfs",   # For S3 access
            "aws",      # For AWS credentials
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


# Create resource instance with extension initialization
duckdb_resource = ExtendedDuckDBResource(
    database=EnvVar("DUCKDB_DATABASE"),
    connection_config={
        "s3_endpoint": EnvVar("AWS_ENDPOINT_URL").get_value().replace("http://", ""),
        "s3_access_key_id": EnvVar("AWS_ACCESS_KEY_ID").get_value(),
        "s3_secret_access_key": EnvVar("AWS_SECRET_ACCESS_KEY").get_value(),
        "s3_region": EnvVar("AWS_REGION").get_value(),
        "s3_use_ssl": False,
        "s3_url_style": "path",
    },
)
