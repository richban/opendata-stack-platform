"""Streamify resource definitions: config, Spark session, and service clients."""

from collections.abc import AsyncIterator, Coroutine, Iterator
from contextlib import asynccontextmanager, contextmanager
from functools import cache
from typing import Any, cast

import clickhouse_connect
import dagster as dg
import redis
import redis.asyncio as aioredis

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import AsyncSchemaRegistryClient
from confluent_kafka.schema_registry.avro import AsyncAvroDeserializer
from dagster_aws.s3 import S3Resource
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from pyspark.sql import SparkSession


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


@cache
def get_streaming_config() -> StreamingJobConfig:
    """Return a singleton ``StreamingJobConfig`` instance from environment."""
    return StreamingJobConfig()


def create_spark_session(
    config: StreamingJobConfig,
    app_name: str = "StreamifyDagsterJob",
) -> SparkSession:
    """Create a SparkSession for Iceberg (Polaris REST catalog) via Spark Connect."""
    polaris_uri = config.polaris_uri
    catalog = config.catalog

    builder = SparkSession.builder.appName(app_name)

    if config.spark_remote:
        builder = builder.remote(config.spark_remote)

    session = (
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

    # Ensure active catalog + namespace context
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{config.namespace}")
    session.sql(f"USE {catalog}.{config.namespace}")

    return session


@dg.resource(required_resource_keys={"streaming_config"})
def spark_resource(context: dg.InitResourceContext) -> SparkSession:
    """Lazy Dagster resource factory for SparkSession."""
    streaming_config: StreamingJobConfig = (
        getattr(context.resources, "streaming_config", None) or get_streaming_config()
    )
    return create_spark_session(streaming_config)


def create_s3_resource(config: StreamingJobConfig) -> S3Resource:
    """Create an S3Resource from Pydantic settings."""
    return S3Resource(
        aws_access_key_id=config.aws_access_key_id,
        aws_secret_access_key=config.aws_secret_access_key,
        endpoint_url=config.aws_endpoint_url,
    )


class ClickHouseResource(dg.ConfigurableResource):
    """Dagster resource for ClickHouse driver-side operations."""

    host: str = Field(default="localhost", description="ClickHouse hostname or IP.")
    port: int = Field(default=8123, description="ClickHouse HTTP port.")
    username: str = Field(default="default", description="ClickHouse username.")
    password: str = Field(default="clickhouse", description="ClickHouse password.")
    database: str = Field(default="streamify", description="ClickHouse database name.")

    def get_client(self) -> clickhouse_connect.driver.Client:
        """Return a new clickhouse-connect Client for driver-side DDL/DML."""
        return clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
        )


def create_clickhouse_resource(
    config: StreamingJobConfig,
) -> ClickHouseResource:
    """Create a ClickHouseResource from Pydantic settings."""
    return ClickHouseResource(
        host=config.clickhouse_host,
        port=config.clickhouse_port,
        username=config.clickhouse_user,
        password=config.clickhouse_password,
        database=config.clickhouse_db,
    )


class RedisResource(dg.ConfigurableResource):
    """Dagster resource for Redis connections (sync and async)."""

    host: str = Field(default="localhost", description="Redis hostname or IP.")
    port: int = Field(default=6379, description="Redis port.")

    def get_sync_client(self) -> redis.Redis:
        """Return a synchronous Redis client."""
        return redis.Redis(host=self.host, port=self.port, decode_responses=True)

    @asynccontextmanager
    async def get_async_client(self) -> AsyncIterator[aioredis.Redis]:
        """Context manager yielding an active async Redis client with cleanup."""
        client = aioredis.Redis(
            host=self.host,
            port=self.port,
            decode_responses=True,
        )
        await client.ping()
        try:
            yield client
        finally:
            await client.aclose()


class SchemaRegistryResource(dg.ConfigurableResource):
    """Dagster resource for Confluent Schema Registry."""

    url: str = Field(default="http://localhost:8081", description="Schema Registry URL.")

    def get_async_client(self) -> AsyncSchemaRegistryClient:
        """Return an AsyncSchemaRegistryClient instance."""
        return AsyncSchemaRegistryClient({"url": self.url})

    async def get_avro_deserializer(
        self,
        from_dict_fn: Any | None = None,
    ) -> AsyncAvroDeserializer:
        """Create an AsyncAvroDeserializer using the configured schema client."""
        client = self.get_async_client()
        return await cast(
            Coroutine[Any, Any, AsyncAvroDeserializer],
            AsyncAvroDeserializer(client, from_dict=from_dict_fn),
        )


class KafkaConsumerResource(dg.ConfigurableResource):
    """Dagster resource for Confluent Kafka Consumer."""

    bootstrap_servers: str = Field(
        default="localhost:9093",
        description="Kafka bootstrap servers connection string.",
    )
    group_id: str = Field(
        default="async-redis-updater-group",
        description="Kafka consumer group ID.",
    )
    auto_offset_reset: str = Field(
        default="earliest",
        description="Kafka auto offset reset policy.",
    )
    enable_auto_commit: bool = Field(
        default=False,
        description="Whether to enable auto commit of offsets.",
    )

    @contextmanager
    def get_consumer(self, topics: list[str]) -> Iterator[Consumer]:
        """Context manager yielding a subscribed Kafka consumer with cleanup on exit."""
        consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": self.group_id,
                "auto.offset.reset": self.auto_offset_reset,
                "enable.auto.commit": self.enable_auto_commit,
            }
        )
        consumer.subscribe(topics)
        try:
            yield consumer
        finally:
            consumer.close()


def create_redis_resource(config: StreamingJobConfig) -> RedisResource:
    """Create a RedisResource from config."""
    return RedisResource(
        host=config.redis_host,
        port=config.redis_port,
    )


def create_schema_registry_resource(
    config: StreamingJobConfig,
) -> SchemaRegistryResource:
    """Create a SchemaRegistryResource from config."""
    return SchemaRegistryResource(
        url=config.schema_registry_url,
    )


def create_kafka_consumer_resource(
    config: StreamingJobConfig,
    group_id: str = "async-redis-updater-group",
) -> KafkaConsumerResource:
    """Create a KafkaConsumerResource from config."""
    return KafkaConsumerResource(
        bootstrap_servers=config.kafka_bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )


@cache
def get_executor_redis_client(host: str, port: int) -> redis.Redis:  # type: ignore[type-arg]
    """Return a cached Redis client for executor use.

    ``@cache`` (thread-safe on CPython) ensures a single client instance is
    reused across micro-batches in the same Python worker process.
    """
    return redis.Redis(host=host, port=port, decode_responses=True)


@cache
def get_executor_clickhouse_client(
    host: str,
    port: int,
    username: str,
    password: str,
    database: str,
) -> clickhouse_connect.driver.Client:
    """Return a cached clickhouse-connect client for executor use.

    Keyed on connection parameters so the client is reused across micro-batches
    in the same Python worker process without re-establishing connections.
    """
    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
    )
