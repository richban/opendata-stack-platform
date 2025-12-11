"""Dagster resources for Spark and Kafka connectivity."""

from dagster import ConfigurableResource
from pydantic import Field


class SparkResource(ConfigurableResource):
    """Resource for Spark session configuration."""

    master_url: str = Field(
        default="spark://spark-master:7077",
        description="Spark master URL",
    )
    app_name: str = Field(
        default="DagsterSparkJob",
        description="Spark application name",
    )
    executor_memory: str = Field(
        default="2g",
        description="Executor memory allocation",
    )
    executor_cores: int = Field(
        default=2,
        description="Number of cores per executor",
    )


class KafkaResource(ConfigurableResource):
    """Resource for Kafka connection configuration."""

    bootstrap_servers: str = Field(
        default="kafka:9092",
        description="Kafka bootstrap servers",
    )
    consumer_group: str = Field(
        default="dagster-streaming-pipeline",
        description="Kafka consumer group ID",
    )


class DataLakeResource(ConfigurableResource):
    """Resource for data lake storage configuration."""

    base_path: str = Field(
        default="/data/lake",
        description="Base path for data lake storage",
    )
    checkpoint_path: str = Field(
        default="/data/checkpoints",
        description="Base path for streaming checkpoints",
    )


class IcebergCatalogResource(ConfigurableResource):
    """Resource for Iceberg catalog configuration."""

    catalog_name: str = Field(
        default="local",
        description="Iceberg catalog name",
    )
    warehouse_path: str = Field(
        default="/data/warehouse",
        description="Iceberg warehouse path",
    )
    database: str = Field(
        default="streamify",
        description="Default database name",
    )
