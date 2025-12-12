"""Dagster resources for Streamify using Spark Connect."""

import os

import dagster as dg
from pyspark.sql import SparkSession


class SparkConnectResource(dg.ConfigurableResource):
    """Spark Connect resource for lazy SparkSession creation.

    Only creates the SparkSession when first accessed during asset execution.
    """

    spark_remote: str = "sc://spark-master"
    polaris_client_id: str = ""
    polaris_client_secret: str = ""

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
                    "spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog"
                )
                .config("spark.sql.catalog.lakehouse.type", "rest")
                .config(
                    "spark.sql.catalog.lakehouse.uri", "http://polaris:8181/api/catalog"
                )
                .config("spark.sql.catalog.lakehouse.warehouse", "lakehouse")
                .config("spark.sql.catalog.lakehouse.credential", polaris_credential)
                .config("spark.sql.catalog.lakehouse.scope", "PRINCIPAL_ROLE:ALL")
                .config(
                    "spark.sql.catalog.lakehouse.header.X-Iceberg-Access-Delegation",
                    "vended-credentials",
                )
                .config("spark.sql.catalog.lakehouse.token-refresh-enabled", "true")
                .config("spark.sql.defaultCatalog", "lakehouse")
                .getOrCreate()
            )

        return self._session


def create_spark_resource():
    """Create SparkConnectResource from environment variables."""
    return SparkConnectResource(
        spark_remote=os.getenv("SPARK_REMOTE", "sc://spark-master"),
        polaris_client_id=os.getenv("POLARIS_CLIENT_ID", ""),
        polaris_client_secret=os.getenv("POLARIS_CLIENT_SECRET", ""),
    )
