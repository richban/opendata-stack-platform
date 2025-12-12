"""Common utilities for Spark scripts."""

import os

from argparse import ArgumentParser
from contextlib import contextmanager

import boto3

from dagster_pipes import (
    PipesCliArgsParamsLoader,
    PipesS3ContextLoader,
    PipesS3MessageWriter,
    open_dagster_pipes,
)
from pyspark.sql import SparkSession


def get_common_arg_parser(description: str) -> ArgumentParser:
    """Create an argument parser with common Spark/Iceberg arguments."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--polaris-uri",
        default="http://polaris:8181/api/catalog",
        help="Polaris catalog URI",
    )
    parser.add_argument(
        "--polaris-credential",
        required=True,
        help="Polaris credential in format client_id:client_secret",
    )
    parser.add_argument(
        "--catalog",
        default="lakehouse",
        help="Iceberg catalog name",
    )
    parser.add_argument(
        "--namespace",
        default="streamify",
        help="Iceberg namespace (database)",
    )
    # Dagster Pipes args
    parser.add_argument("--dagster-pipes-context", help="Pipes context (auto)")
    parser.add_argument("--dagster-pipes-messages", help="Pipes messages (auto)")

    return parser


def create_spark_session(app_name: str, args) -> SparkSession:
    """Create SparkSession configured for Iceberg and Polaris."""
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{args.catalog}", "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(f"spark.sql.catalog.{args.catalog}.type", "rest")
        .config(f"spark.sql.catalog.{args.catalog}.uri", args.polaris_uri)
        .config(f"spark.sql.catalog.{args.catalog}.warehouse", args.catalog)
        .config(f"spark.sql.catalog.{args.catalog}.credential", args.polaris_credential)
        .config(f"spark.sql.catalog.{args.catalog}.scope", "PRINCIPAL_ROLE:ALL")
        .config(
            f"spark.sql.catalog.{args.catalog}.header.X-Iceberg-Access-Delegation",
            "vended-credentials",
        )
        .config(f"spark.sql.catalog.{args.catalog}.token-refresh-enabled", "true")
        .config("spark.sql.defaultCatalog", args.catalog)
        .getOrCreate()
    )


def get_s3_client():
    """Create Boto3 S3 client for Dagster Pipes."""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


class DummyPipesContext:
    """Dummy context when Dagster Pipes is not available."""

    def __init__(self):
        self.log = self

    def info(self, msg):
        print(f"[INFO] {msg}")

    def report_asset_materialization(self, metadata=None):
        pass


@contextmanager
def open_pipes(args):
    """
    Context manager for Dagster Pipes.
    Yields a pipes context object (real or dummy).
    """
    if PIPES_AVAILABLE and args.dagster_pipes_context:
        s3_client = get_s3_client()
        with open_dagster_pipes(
            message_writer=PipesS3MessageWriter(client=s3_client),
            context_loader=PipesS3ContextLoader(client=s3_client),
            params_loader=PipesCliArgsParamsLoader(),
        ) as pipes:
            yield pipes
    else:
        if not PIPES_AVAILABLE:
            print("[WARN] dagster_pipes not installed, running without Pipes integration")
        yield DummyPipesContext()
