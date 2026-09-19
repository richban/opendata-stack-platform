"""Shared configuration and connection utilities for Iceberg/Polaris notebooks."""

from __future__ import annotations

import logging
import socket
import urllib.parse

from dataclasses import dataclass

import duckdb
import ibis

from obstore.store import S3Store
from pyiceberg.catalog.rest import RestCatalog
from pyspark.sql import SparkSession

from streamify.defs.resources import StreamingJobConfig, get_streaming_config

# Configure module logger
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def _normalize_endpoint(url: str) -> str:
    """Normalize docker-internal hostnames to localhost if unreachable/unresolvable."""
    if not url:
        return url
    try:
        parsed = urllib.parse.urlsplit(url)
        if parsed.hostname:
            try:
                socket.gethostbyname(parsed.hostname)
            except (socket.gaierror, OSError):
                if parsed.hostname in (
                    "polaris",
                    "minio",
                    "kafka",
                    "redis",
                    "clickhouse",
                    "spark-master",
                    "spark-connect",
                ):
                    netloc = parsed.netloc.replace(parsed.hostname, "localhost")
                    return urllib.parse.urlunsplit(parsed._replace(netloc=netloc))
    except Exception:
        pass
    return url


@dataclass(frozen=True)
class PolarisConfig:
    """Polaris REST catalog configuration."""

    client_id: str
    client_secret: str
    catalog: str
    uri: str


@dataclass(frozen=True)
class MinioConfig:
    """MinIO S3-compatible storage configuration."""

    endpoint: str
    access_key: str
    secret_key: str


def get_polaris_config(config: StreamingJobConfig | None = None) -> PolarisConfig:
    """Load Polaris configuration from StreamingJobConfig.

    Raises:
        ValueError: If required credentials are not set.

    Returns:
        PolarisConfig with connection parameters.
    """
    if config is None:
        config = get_streaming_config()

    client_id = config.polaris_client_id
    client_secret = config.polaris_client_secret
    catalog = config.catalog
    uri = _normalize_endpoint(config.polaris_uri)

    if not client_id:
        raise ValueError("POLARIS_CLIENT_ID is not set in environment or .env files")
    if not client_secret:
        raise ValueError("POLARIS_CLIENT_SECRET is not set in environment or .env files")

    logger.info("Polaris config: uri=%s, catalog=%s", uri, catalog)

    return PolarisConfig(
        client_id=client_id,
        client_secret=client_secret,
        catalog=catalog,
        uri=uri,
    )


def get_minio_config(config: StreamingJobConfig | None = None) -> MinioConfig:
    """Load MinIO configuration from StreamingJobConfig.

    Returns:
        MinioConfig with connection parameters.
    """
    if config is None:
        config = get_streaming_config()

    endpoint = _normalize_endpoint(config.aws_endpoint_url)
    access_key = config.aws_access_key_id
    secret_key = config.aws_secret_access_key

    logger.info("MinIO config: endpoint=%s", endpoint)

    return MinioConfig(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
    )


def get_s3_store(minio: MinioConfig | None = None) -> S3Store:
    if minio is None:
        minio = get_minio_config()

    store = S3Store(
        "lakehouse",
        access_key_id=minio.access_key,
        secret_access_key=minio.secret_key,
        endpoint_url=minio.endpoint,
    )

    return store


def create_duckdb_connection(
    polaris: PolarisConfig | None = None,
    minio: MinioConfig | None = None,
) -> duckdb.DuckDBPyConnection:
    """Create and configure DuckDB connection with Iceberg catalog attached.

    Sets up:
    - iceberg and httpfs extensions
    - OAuth2 secret for Polaris authentication
    - S3 secret for MinIO access
    - Attached Iceberg REST catalog

    Args:
        polaris: Polaris configuration. Uses environment variables if not provided.
        minio: MinIO configuration. Uses environment variables if not provided.

    Returns:
        Configured DuckDB connection with catalog attached as 'lakehouse'.
    """
    if polaris is None:
        polaris = get_polaris_config()
    if minio is None:
        minio = get_minio_config()

    logger.info("Creating DuckDB connection...")

    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("INSTALL httpfs;  LOAD httpfs;")
    logger.debug("DuckDB extensions loaded: iceberg, httpfs")

    # OAuth2 secret — DuckDB exchanges client_id/secret for a bearer token.
    # OAUTH2_SERVER_URI needed because Polaris puts its token endpoint at
    # /api/catalog/v1/oauth/tokens, not at the catalog root.
    con.execute(f"""
        CREATE OR REPLACE SECRET polaris_secret (
            TYPE              iceberg,
            CLIENT_ID         '{polaris.client_id}',
            CLIENT_SECRET     '{polaris.client_secret}',
            OAUTH2_SCOPE      'PRINCIPAL_ROLE:ALL',
            OAUTH2_SERVER_URI '{polaris.uri}/v1/oauth/tokens'
        )
    """)
    logger.debug("Created polaris_secret for OAuth2 authentication")

    # S3 secret for MinIO.
    # Polaris catalog config has stsUnavailable=true, so vended (STS) credentials
    # won't work. We use static MinIO credentials scoped to s3://lakehouse/.
    minio_host = minio.endpoint.replace("http://", "").replace("https://", "")
    con.execute(f"""
        CREATE OR REPLACE SECRET minio_secret (
            TYPE      s3,
            KEY_ID    '{minio.access_key}',
            SECRET    '{minio.secret_key}',
            ENDPOINT  '{minio_host}',
            SCOPE     's3://lakehouse',
            URL_STYLE 'path',
            USE_SSL   false
        )
    """)
    logger.debug("Created minio_secret for S3 access")

    # ATTACH: ACCESS_DELEGATION_MODE 'none' is critical here.
    # Default is 'vended_credentials' which makes DuckDB request temporary STS creds
    # from Polaris — but our Polaris has stsUnavailable=true and MinIO runs on a
    # Docker-internal hostname (minio:9000) unreachable from the host.
    # 'none' tells DuckDB to use minio_secret directly for all s3:// data file reads.
    con.execute(f"""
        ATTACH '{polaris.catalog}' AS lakehouse (
            TYPE                   iceberg,
            ENDPOINT               '{polaris.uri}',
            SECRET                 'polaris_secret',
            ACCESS_DELEGATION_MODE 'none'
        )
    """)

    logger.info("DuckDB attached to Polaris catalog '%s'", polaris.catalog)

    # Log available tables
    tables_df = con.execute("SHOW ALL TABLES").df()
    logger.info("Available tables:\n%s", tables_df.to_string())

    return con


def create_iceberg_catalog(
    polaris: PolarisConfig | None = None,
    minio: MinioConfig | None = None,
) -> RestCatalog:
    """Create PyIceberg REST catalog connection.

    Args:
        polaris: Polaris configuration. Uses environment variables if not provided.
        minio: MinIO configuration for S3 credentials. Uses environment variables
            if not provided.

    Returns:
        Configured PyIceberg RestCatalog.
    """
    if polaris is None:
        polaris = get_polaris_config()
    if minio is None:
        minio = get_minio_config()

    logger.info("Creating PyIceberg REST catalog '%s'...", polaris.catalog)

    catalog = RestCatalog(
        name=polaris.catalog,
        **{
            "uri": polaris.uri,
            "warehouse": polaris.catalog,
            "credential": f"{polaris.client_id}:{polaris.client_secret}",
            "scope": "PRINCIPAL_ROLE:ALL",
            "oauth2-server-uri": f"{polaris.uri.rstrip('/')}/v1/oauth/tokens",
            # S3 configuration for MinIO - use static credentials, disable vending.
            "s3.endpoint": minio.endpoint,
            "s3.access-key-id": minio.access_key,
            "s3.secret-access-key": minio.secret_key,
            "s3.remote-signing-enabled": "false",
            # MinIO requires path-style access (localhost:9000/bucket) instead
            # of virtual-hosted-style (bucket.localhost:9000) which would fail
            # DNS resolution.
            "s3.path-style-access": "true",
            # Disable credential vending: PyIceberg sends
            # "X-Iceberg-Access-Delegation: vended-credentials" by default,
            # which causes Polaris to attempt STS token generation.
            # MinIO does not support STS, so we override the header to an empty
            # string to suppress the request entirely and fall back to the
            # static s3.* credentials configured above.
            "header.X-Iceberg-Access-Delegation": "",
        },
    )

    logger.info("Catalog initialized successfully")

    return catalog


def create_spark_session(
    spark_connect_uri: str | None = None,
    config: StreamingJobConfig | None = None,
) -> tuple[SparkSession, ibis.BaseBackend]:
    """Create Spark session and Ibis connection to remote Spark Connect server.

    The Spark Connect server is pre-configured with Iceberg catalog settings
    in docker-compose.yml, so the client only needs to connect via remote().
    The Polaris catalog 'lakehouse' is already configured server-side.

    Args:
        spark_connect_uri: Spark Connect server URI (default: from config.spark_remote or sc://localhost:15002)
        config: StreamingJobConfig instance (default: get_streaming_config())

    Returns:
        Tuple of (SparkSession, IbisSparkBackend) connected to the remote server
        with Iceberg catalog ready.
    """
    if config is None:
        config = get_streaming_config()

    if spark_connect_uri is None:
        spark_connect_uri = config.spark_remote or "sc://localhost:15002"
    spark_connect_uri = _normalize_endpoint(spark_connect_uri)

    logger.info("Connecting to Spark Connect server at %s...", spark_connect_uri)

    # Connect to the remote server - catalog is already configured server-side
    spark = SparkSession.builder.remote(spark_connect_uri).getOrCreate()

    # Create Ibis connection from the Spark session
    logger.info("Creating Ibis PySpark connection...")
    spark_conn = ibis.pyspark.connect(spark)

    logger.info("Spark session and Ibis connection created successfully")
    logger.info("Spark UI: http://localhost:4041")

    return spark, spark_conn


def create_delta_spark_session(
    warehouse_dir: str,
    app_name: str = "delta-local",
) -> tuple[SparkSession, ibis.BaseBackend]:
    """Create a local Spark session configured for Delta Lake on MinIO via S3A.

    This bypasses Polaris/Iceberg entirely and writes Delta tables directly to
    the provided S3A path using static MinIO credentials.

    Args:
        warehouse_dir: Root path for the Spark warehouse (e.g. "s3a://lakehouse/delta").
                       This is set as spark.sql.warehouse.dir.
        app_name: Spark application name.

    Returns:
        Tuple of (SparkSession, IbisPySparkBackend) ready for Delta operations.
    """
    minio = get_minio_config()

    logger.info("Creating local Delta Spark session (warehouse_dir=%s)...", warehouse_dir)

    builder = (
        SparkSession.Builder()
        .master("local[*]")
        .appName(app_name)
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_4.0_2.13:4.2.0,"
            "org.apache.hadoop:hadoop-aws:3.4.1,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.ui.enabled", "true")
        .config("spark.hadoop.fs.s3a.endpoint", minio.endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )

    spark = builder.getOrCreate()

    logger.info("Creating Ibis PySpark connection...")
    spark_conn = ibis.pyspark.connect(spark)

    logger.info("Delta Spark session and Ibis connection created successfully")

    return spark, spark_conn
