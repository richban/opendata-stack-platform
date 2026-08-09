"""Streamify resource definitions: config, Spark session, and service clients."""

from functools import cache

import clickhouse_connect
import dagster as dg
import redis

from dagster_aws.s3 import S3Resource
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from pyspark.sql import SparkSession

from streamify.clients import (
    get_executor_clickhouse_client,
    get_executor_redis_client,
)


class StreamingJobConfig(BaseSettings, dg.ConfigurableResource):
    """Configuration for Spark Structured Streaming, S3, Polaris, and Dagster jobs.

    Populated from environment variables (via ``pydantic-settings``) when used
    stand-alone, or from Dagster's resource system when used as a
    ``ConfigurableResource``.
    """

    model_config = SettingsConfigDict(
        case_sensitive=False,
        extra="ignore",
        populate_by_name=True,
        env_file=(".env", ".env.dev", ".env.polaris"),
    )

    # ------------------------------------------------------------------ Kafka
    kafka_bootstrap_servers: str = Field(
        default="localhost:9093",
        validation_alias="KAFKA_BOOTSTRAP_SERVERS",
    )
    executor_kafka_bootstrap_servers: str = Field(
        default="kafka:9092",
        validation_alias="EXECUTOR_KAFKA_BOOTSTRAP_SERVERS",
    )
    max_offsets_per_trigger: int = Field(
        default=100_000,
        validation_alias="MAX_OFFSETS_PER_TRIGGER",
    )

    # ------------------------------------------------------------------ Storage / catalog
    checkpoint_path: str = Field(
        default="s3a://checkpoints/streaming",
        validation_alias="CHECKPOINT_PATH",
    )
    songs_catalog_path: str = Field(
        default="s3a://datalake/seeds/songs.csv",
        validation_alias="SONGS_CATALOG_PATH",
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

    # ------------------------------------------------------------------ Dagster / S3
    dagster_pipes_bucket: str = Field(
        default="dagster-pipes",
        validation_alias="DAGSTER_PIPES_BUCKET",
    )
    schema_registry_url: str = Field(
        default="http://localhost:8081",
        validation_alias="SCHEMA_REGISTRY_URL",
    )

    # ------------------------------------------------------------------ Redis
    redis_host: str = Field(
        default="localhost",
        validation_alias="REDIS_HOST",
    )
    executor_redis_host: str = Field(
        default="redis",
        validation_alias="EXECUTOR_REDIS_HOST",
    )
    redis_port: int = Field(
        default=6379,
        validation_alias="REDIS_PORT",
    )

    # ------------------------------------------------------------------ ClickHouse
    clickhouse_host: str = Field(
        default="localhost",
        validation_alias="CLICKHOUSE_HOST",
    )
    executor_clickhouse_host: str = Field(
        default="clickhouse",
        validation_alias="EXECUTOR_CLICKHOUSE_HOST",
    )
    clickhouse_port: int = Field(
        default=8123,
        validation_alias="CLICKHOUSE_PORT",
    )
    clickhouse_db: str = Field(
        default="streamify",
        validation_alias="CLICKHOUSE_DB",
    )
    clickhouse_user: str = Field(
        default="default",
        validation_alias="CLICKHOUSE_USER",
    )
    clickhouse_password: str = Field(
        default="clickhouse",
        validation_alias="CLICKHOUSE_PASSWORD",
    )

    # ------------------------------------------------------------------ Spark / AWS
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

    # ------------------------------------------------------------------ Trigger intervals
    iceberg_trigger_interval: str = Field(
        default="30 seconds",
        validation_alias="ICEBERG_TRIGGER_INTERVAL",
    )
    clickhouse_trigger_interval: str = Field(
        default="10 seconds",
        validation_alias="CLICKHOUSE_TRIGGER_INTERVAL",
    )

    # ---------------------------------------------------------------- Computed properties

    @property
    def polaris_credential(self) -> str:
        """Formatted ``client_id:client_secret`` string for Polaris auth."""
        return f"{self.polaris_client_id}:{self.polaris_client_secret}"


# ------------------------------------------------------------------
# Executor-side Redis client pool
# ------------------------------------------------------------------


# ------------------------------------------------------------------
# Singleton config / session factories
# ------------------------------------------------------------------


@cache
def get_streaming_config() -> StreamingJobConfig:
    """Return a singleton ``StreamingJobConfig`` instance from environment."""
    return StreamingJobConfig()


def create_spark_session(
    app_name: str = "StreamifyDagsterJob",
    config: StreamingJobConfig | None = None,
) -> SparkSession:
    """Create a SparkSession for Iceberg (Polaris REST catalog) via Spark Connect."""
    if config is None:
        config = get_streaming_config()

    polaris_uri = config.polaris_uri
    catalog = config.catalog

    builder = SparkSession.builder.appName(app_name)

    if config.spark_remote:
        builder = builder.remote(config.spark_remote)

    return (
        builder.config(
            f"spark.sql.catalog.{catalog}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{catalog}.type", "rest")
        .config(f"spark.sql.catalog.{catalog}.uri", polaris_uri)
        .config(
            f"spark.sql.catalog.{catalog}.oauth2-server-uri",
            f"{polaris_uri}/v1/oauth/tokens",
        )
        .config(f"spark.sql.catalog.{catalog}.warehouse", catalog)
        .config(f"spark.sql.catalog.{catalog}.credential", config.polaris_credential)
        .config(f"spark.sql.catalog.{catalog}.scope", "PRINCIPAL_ROLE:ALL")
        .config(f"spark.sql.catalog.{catalog}.s3.endpoint", "http://minio:9000")
        .config(
            f"spark.sql.catalog.{catalog}.s3.access-key-id",
            config.aws_access_key_id,
        )
        .config(
            f"spark.sql.catalog.{catalog}.s3.secret-access-key",
            config.aws_secret_access_key,
        )
        .config(f"spark.sql.catalog.{catalog}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{catalog}.token-refresh-enabled", "true")
        .config("spark.sql.defaultCatalog", catalog)
        .config("spark.executorEnv.PYTHONPATH", "/opt/streamify/src")
        .getOrCreate()
    )


def create_s3_resource(config: StreamingJobConfig | None = None) -> S3Resource:
    """Create an S3Resource from Pydantic settings."""
    if config is None:
        config = get_streaming_config()

    return S3Resource(
        aws_access_key_id=config.aws_access_key_id,
        aws_secret_access_key=config.aws_secret_access_key,
        endpoint_url=config.aws_endpoint_url,
    )


def create_clickhouse_resource(
    config: StreamingJobConfig | None = None,
) -> clickhouse_connect.driver.Client:
    """Create a *new* ``clickhouse-connect`` client from Pydantic settings.

    This is intentionally uncached: it is used by the driver for short-lived
    DDL operations.
    """
    if config is None:
        config = get_streaming_config()

    return clickhouse_connect.get_client(
        host=config.clickhouse_host,
        port=config.clickhouse_port,
        username=config.clickhouse_user,
        password=config.clickhouse_password,
    )
