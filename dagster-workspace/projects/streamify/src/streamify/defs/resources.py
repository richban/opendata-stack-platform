from functools import lru_cache

import dagster as dg

from dagster_aws.s3 import S3Resource
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from pyspark.sql import SparkSession


class StreamingJobConfig(BaseSettings, dg.ConfigurableResource):
    """Configuration for Spark Structured Streaming, S3, Polaris, and Dagster jobs."""

    model_config = SettingsConfigDict(
        case_sensitive=False,
        extra="ignore",
        populate_by_name=True,
    )

    kafka_bootstrap_servers: str = Field(
        default="localhost:9093",
        validation_alias="KAFKA_BOOTSTRAP_SERVERS",
    )
    checkpoint_path: str = Field(
        default="s3a://checkpoints/streaming",
        validation_alias="CHECKPOINT_PATH",
    )
    polaris_uri: str = Field(
        default="http://localhost:8181/api/catalog",
        validation_alias="POLARIS_URI",
    )
    polaris_client_id: str = Field(
        default="",
        validation_alias="POLARIS_CLIENT_ID",
    )
    polaris_client_secret: str = Field(
        default="",
        validation_alias="POLARIS_CLIENT_SECRET",
    )
    catalog: str = Field(
        default="lakehouse",
        validation_alias="POLARIS_CATALOG",
    )
    namespace: str = Field(
        default="streamify",
        validation_alias="POLARIS_NAMESPACE",
    )
    dagster_pipes_bucket: str = Field(
        default="dagster-pipes",
        validation_alias="DAGSTER_PIPES_BUCKET",
    )
    schema_registry_url: str = Field(
        default="http://localhost:8081",
        validation_alias="SCHEMA_REGISTRY_URL",
    )
    redis_host: str = Field(
        default="localhost",
        validation_alias="REDIS_HOST",
    )
    redis_port: int = Field(
        default=6379,
        validation_alias="REDIS_PORT",
    )
    spark_remote: str = Field(
        default="sc://localhost:15002",
        validation_alias="SPARK_REMOTE",
    )
    aws_access_key_id: str = Field(
        default="minioadmin",
        validation_alias="AWS_ACCESS_KEY_ID",
    )
    aws_secret_access_key: str = Field(
        default="minioadmin",
        validation_alias="AWS_SECRET_ACCESS_KEY",
    )
    aws_endpoint_url: str = Field(
        default="http://localhost:9000",
        validation_alias="AWS_ENDPOINT_URL",
    )

    def get_polaris_credential(self) -> str:
        """Get formatted Polaris credential string."""
        return f"{self.polaris_client_id}:{self.polaris_client_secret}"


@lru_cache(maxsize=1)
def get_streaming_config() -> StreamingJobConfig:
    """Return a singleton StreamingJobConfig instance from environment."""
    return StreamingJobConfig()


def create_spark_session(
    app_name: str = "StreamifyDagsterJob",
    config: StreamingJobConfig = None,
) -> SparkSession:
    """Create a SparkSession for Iceberg and Spark Connect using Pydantic settings."""
    if config is None:
        config = get_streaming_config()

    config_polaris_uri = (
        config.polaris_uri.replace("localhost", "polaris")
        .replace("127.0.0.1", "polaris")
        .replace("192.168.1.47", "polaris")
    )
    if not config_polaris_uri:
        config_polaris_uri = "http://polaris:8181/api/catalog"

    builder = SparkSession.builder.appName(app_name)

    if config.spark_remote:
        builder = builder.remote(config.spark_remote)

    return (
        builder.config(
            f"spark.sql.catalog.{config.catalog}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{config.catalog}.type", "rest")
        .config(f"spark.sql.catalog.{config.catalog}.uri", config_polaris_uri)
        .config(
            f"spark.sql.catalog.{config.catalog}.oauth2-server-uri",
            f"{config_polaris_uri}/v1/oauth/tokens",
        )
        .config(f"spark.sql.catalog.{config.catalog}.warehouse", config.catalog)
        .config(
            f"spark.sql.catalog.{config.catalog}.credential",
            config.get_polaris_credential(),
        )
        .config(f"spark.sql.catalog.{config.catalog}.scope", "PRINCIPAL_ROLE:ALL")
        .config(
            f"spark.sql.catalog.{config.catalog}.s3.endpoint",
            "http://minio:9000",
        )
        .config(
            f"spark.sql.catalog.{config.catalog}.s3.access-key-id",
            config.aws_access_key_id,
        )
        .config(
            f"spark.sql.catalog.{config.catalog}.s3.secret-access-key",
            config.aws_secret_access_key,
        )
        .config(f"spark.sql.catalog.{config.catalog}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{config.catalog}.token-refresh-enabled", "true")
        .config("spark.sql.defaultCatalog", config.catalog)
        .getOrCreate()
    )


def create_s3_resource(config: StreamingJobConfig = None) -> S3Resource:
    """Create S3 resource using Pydantic settings."""
    if config is None:
        config = get_streaming_config()

    return S3Resource(
        aws_access_key_id=config.aws_access_key_id,
        aws_secret_access_key=config.aws_secret_access_key,
        endpoint_url=config.aws_endpoint_url,
    )
