"""Dagster resources for Streamify using Spark Connect and streaming jobs."""

import os

import dagster as dg
from dagster import EnvVar
from dagster_aws.s3 import S3Resource
from pyspark.sql import SparkSession


class SparkConnectResource(dg.ConfigurableResource):
    """Spark Connect resource for lazy SparkSession creation.

    Only creates the SparkSession when first accessed during asset execution.
    Can also be used to create regular SparkSessions for Pipes scripts.
    """

    spark_remote: str = "sc://spark-master"
    polaris_client_id: str = EnvVar("POLARIS_CLIENT_ID")
    polaris_client_secret: str = EnvVar("POLARIS_CLIENT_SECRET")
    polaris_uri: str = EnvVar("POLARIS_URI")
    catalog: str = EnvVar("POLARIS_CATALOG")

    _session: SparkSession = None

    def get_session(self) -> SparkSession:
        """Get or create SparkSession via Spark Connect."""
        if self._session is None:
            self._session = self._create_session(self.spark_remote)
        return self._session

    def create_regular_session(self, app_name: str = "StreamToIceberg") -> SparkSession:
        """Create a regular (non-Connect) SparkSession for Pipes scripts.

        This is used by streaming scripts that need a full Spark driver,
        not just Spark Connect.
        """
        return self._create_session(None, app_name)

    def _create_session(
        self, spark_remote: str | None = None, app_name: str = "DagsterSparkJob"
    ) -> SparkSession:
        """Internal method to create SparkSession with common Iceberg config."""
        polaris_credential = f"{self.polaris_client_id}:{self.polaris_client_secret}"

        # Ensure Spark Connect (Docker) can reach Polaris (Docker)
        # Use Docker network hostname 'polaris' instead of localhost
        config_polaris_uri = self.polaris_uri.replace("localhost", "polaris").replace(
            "192.168.1.47", "polaris"
        )

        builder = SparkSession.builder.appName(app_name)

        if spark_remote:
            builder = builder.remote(spark_remote)

        return (
            builder.config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(
                f"spark.sql.catalog.{self.catalog}",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config(f"spark.sql.catalog.{self.catalog}.type", "rest")
            .config(f"spark.sql.catalog.{self.catalog}.uri", config_polaris_uri)
            .config(f"spark.sql.catalog.{self.catalog}.warehouse", self.catalog)
            .config(f"spark.sql.catalog.{self.catalog}.credential", polaris_credential)
            .config(
                f"spark.sql.catalog.{self.catalog}.oauth2-server-uri",
                f"{config_polaris_uri}/v1/oauth/tokens",
            )
            .config(f"spark.sql.catalog.{self.catalog}.scope", "PRINCIPAL_ROLE:ALL")
            .config(
                f"spark.sql.catalog.{self.catalog}.s3.endpoint",
                "http://minio:9000",
            )
            .config(
                f"spark.sql.catalog.{self.catalog}.s3.access-key-id",
                os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            )
            .config(
                f"spark.sql.catalog.{self.catalog}.s3.secret-access-key",
                os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            )
            .config(f"spark.sql.catalog.{self.catalog}.s3.path-style-access", "true")
            .config(f"spark.sql.catalog.{self.catalog}.token-refresh-enabled", "true")
            .config("spark.sql.defaultCatalog", self.catalog)
            .getOrCreate()
        )


class StreamingJobConfig(dg.ConfigurableResource):
    """Configuration for Spark Structured Streaming jobs.

    This resource provides all configuration needed for streaming jobs,
    eliminating the need for argparse in the streaming script.
    """

    kafka_bootstrap_servers: str = "kafka:9092"
    checkpoint_path: str = "s3a://checkpoints/streaming"
    polaris_uri: str = EnvVar("POLARIS_URI")
    polaris_client_id: str = EnvVar("POLARIS_CLIENT_ID")
    polaris_client_secret: str = EnvVar("POLARIS_CLIENT_SECRET")
    catalog: str = EnvVar("POLARIS_CATALOG")
    namespace: str = EnvVar("POLARIS_NAMESPACE")
    dagster_pipes_bucket: str = EnvVar("DAGSTER_PIPES_BUCKET")

    def get_polaris_credential(self) -> str:
        """Get formatted Polaris credential string."""
        return f"{self.polaris_client_id}:{self.polaris_client_secret}"


def create_spark_resource():
    """Create SparkConnectResource from environment variables."""
    return SparkConnectResource(
        spark_remote=os.getenv("SPARK_REMOTE", "sc://localhost:15002"),
        polaris_client_id=os.getenv("POLARIS_CLIENT_ID", ""),
        polaris_client_secret=os.getenv("POLARIS_CLIENT_SECRET", ""),
        polaris_uri=os.getenv("POLARIS_URI", ""),
        catalog=os.getenv("POLARIS_CATALOG", ""),
    )


def create_streaming_config():
    """Create StreamingJobConfig from environment variables."""
    return StreamingJobConfig(
        kafka_bootstrap_servers=EnvVar("KAFKA_BOOTSTRAP_SERVERS")
        .get_value()
        .replace("localhost", "kafka")
        .replace("192.168.1.47", "kafka"),
        checkpoint_path=EnvVar("CHECKPOINT_PATH").get_value(),
        polaris_uri=EnvVar("POLARIS_URI").get_value(),
        polaris_client_id=EnvVar("POLARIS_CLIENT_ID").get_value(),
        polaris_client_secret=EnvVar("POLARIS_CLIENT_SECRET").get_value(),
        catalog=EnvVar("POLARIS_CATALOG").get_value(),
        namespace=EnvVar("POLARIS_NAMESPACE").get_value(),
    )


def create_s3_resource():
    """Create S3 resource with environment-aware endpoint configuration."""
    return S3Resource(
        aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID").get_value(),
        aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY").get_value(),
        endpoint_url=EnvVar("AWS_ENDPOINT_URL").get_value(),
    )
