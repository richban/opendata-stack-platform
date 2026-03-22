"""Dagster resources for Streamify using Spark Connect and streaming jobs."""

import os

from dagster import EnvVar
from dagster_aws.s3 import S3Resource
from pyspark.sql import SparkSession

import dagster as dg


def create_spark_session(app_name: str = "StreamifyDagsterJob") -> SparkSession:
    """Create a SparkSession configured for Iceberg and Spark Connect via environment variables."""
    # Ensure Spark Connect (Docker) can reach Polaris (Docker)
    # Use Docker network hostname 'polaris' instead of localhost
    polaris_uri = os.getenv("POLARIS_URI", "")
    config_polaris_uri = polaris_uri.replace("localhost", "polaris").replace(
        "192.168.1.47", "polaris"
    )

    polaris_client_id = os.getenv("POLARIS_CLIENT_ID", "")
    polaris_client_secret = os.getenv("POLARIS_CLIENT_SECRET", "")
    polaris_credential = f"{polaris_client_id}:{polaris_client_secret}"

    catalog = os.getenv("POLARIS_CATALOG", "lakehouse")
    spark_remote = os.getenv("SPARK_REMOTE", "sc://localhost:15002")

    builder = SparkSession.builder.appName(app_name)

    if spark_remote:
        builder = builder.remote(spark_remote)

    return (
        builder.config(
            f"spark.sql.catalog.{catalog}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{catalog}.type", "rest")
        .config(f"spark.sql.catalog.{catalog}.uri", config_polaris_uri)
        .config(f"spark.sql.catalog.{catalog}.warehouse", catalog)
        .config(f"spark.sql.catalog.{catalog}.credential", polaris_credential)
        .config(
            f"spark.sql.catalog.{catalog}.oauth2-server-uri",
            f"{config_polaris_uri}/v1/oauth/tokens",
        )
        .config(f"spark.sql.catalog.{catalog}.scope", "PRINCIPAL_ROLE:ALL")
        .config(
            f"spark.sql.catalog.{catalog}.s3.endpoint",
            "http://minio:9000",
        )
        .config(
            f"spark.sql.catalog.{catalog}.s3.access-key-id",
            os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        )
        .config(
            f"spark.sql.catalog.{catalog}.s3.secret-access-key",
            os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        )
        .config(f"spark.sql.catalog.{catalog}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{catalog}.token-refresh-enabled", "true")
        .config("spark.sql.defaultCatalog", catalog)
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
