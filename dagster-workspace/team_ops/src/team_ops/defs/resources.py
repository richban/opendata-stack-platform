"""Dagster resources for Streamify using Spark Connect and streaming jobs."""

import os

import dagster as dg
from dagster import EnvVar
from dagster_aws.s3 import S3Resource
from pyspark.sql import SparkSession


class SparkConnectResource(dg.ConfigurableResource):
    """Spark Connect resource for lazy SparkSession creation.

    Only creates the SparkSession when first accessed during asset execution.
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
            polaris_credential = f"{self.polaris_client_id}:{self.polaris_client_secret}"

            self._session = (
                SparkSession.builder.remote(self.spark_remote)
                .config(
                    "spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                )
                .config(
                    f"spark.sql.catalog.{self.catalog}",
                    "org.apache.iceberg.spark.SparkCatalog",
                )
                .config(f"spark.sql.catalog.{self.catalog}.type", "rest")
                .config(f"spark.sql.catalog.{self.catalog}.uri", self.polaris_uri)
                .config(f"spark.sql.catalog.{self.catalog}.warehouse", self.catalog)
                .config(
                    f"spark.sql.catalog.{self.catalog}.credential", polaris_credential
                )
                .config(f"spark.sql.catalog.{self.catalog}.scope", "PRINCIPAL_ROLE:ALL")
                .config(
                    f"spark.sql.catalog.{self.catalog}.header.X-Iceberg-Access-Delegation",
                    "vended-credentials",
                )
                .config(f"spark.sql.catalog.{self.catalog}.token-refresh-enabled", "true")
                .config("spark.sql.defaultCatalog", self.catalog)
                .getOrCreate()
            )

        return self._session


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

    def get_polaris_credential(self) -> str:
        """Get formatted Polaris credential string."""
        return f"{self.polaris_client_id}:{self.polaris_client_secret}"


def create_spark_resource():
    """Create SparkConnectResource from environment variables."""
    return SparkConnectResource(
        spark_remote=EnvVar("SPARK_REMOTE"),
        polaris_client_id=EnvVar("POLARIS_CLIENT_ID"),
        polaris_client_secret=EnvVar("POLARIS_CLIENT_SECRET"),
        polaris_uri=EnvVar("POLARIS_URI"),
        catalog=EnvVar("POLARIS_CATALOG"),
    )


def create_streaming_config():
    """Create StreamingJobConfig from environment variables."""
    return StreamingJobConfig(
        kafka_bootstrap_servers=EnvVar("KAFKA_BOOTSTRAP_SERVERS"),
        checkpoint_path=EnvVar("CHECKPOINT_PATH"),
        polaris_uri=EnvVar("POLARIS_URI"),
        polaris_client_id=EnvVar("POLARIS_CLIENT_ID"),
        polaris_client_secret=EnvVar("POLARIS_CLIENT_SECRET"),
        catalog=EnvVar("POLARIS_CATALOG"),
        namespace=EnvVar("POLARIS_NAMESPACE"),
    )


def create_s3_resource():
    """Create S3 resource with environment-aware endpoint configuration."""
    return S3Resource(
        aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
        endpoint_url=EnvVar("AWS_ENDPOINT_URL"),
    )
