"""Shared configuration and connection utilities for Iceberg/Polaris notebooks."""

from __future__ import annotations

import logging
import os

from dataclasses import dataclass

import duckdb

from pyiceberg.catalog.rest import RestCatalog

# Configure module logger
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


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


def get_polaris_config() -> PolarisConfig:
    """Load Polaris configuration from environment variables.

    Raises:
        ValueError: If required environment variables are not set.

    Returns:
        PolarisConfig with connection parameters.
    """
    client_id = os.getenv("POLARIS_CLIENT_ID")
    client_secret = os.getenv("POLARIS_CLIENT_SECRET")
    catalog = os.getenv("POLARIS_CATALOG", "lakehouse")
    uri = os.getenv("POLARIS_URI", "http://localhost:8181/api/catalog")

    if not client_id:
        raise ValueError("POLARIS_CLIENT_ID environment variable is not set")
    if not client_secret:
        raise ValueError("POLARIS_CLIENT_SECRET environment variable is not set")

    logger.info("Polaris config: uri=%s, catalog=%s", uri, catalog)

    return PolarisConfig(
        client_id=client_id,
        client_secret=client_secret,
        catalog=catalog,
        uri=uri,
    )


def get_minio_config() -> MinioConfig:
    """Load MinIO configuration from environment variables.

    Returns:
        MinioConfig with connection parameters.
    """
    endpoint = os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

    logger.info("MinIO config: endpoint=%s", endpoint)

    return MinioConfig(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
    )


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
) -> RestCatalog:
    """Create PyIceberg REST catalog connection.

    Args:
        polaris: Polaris configuration. Uses environment variables if not provided.

    Returns:
        Configured PyIceberg RestCatalog.
    """
    if polaris is None:
        polaris = get_polaris_config()

    logger.info("Creating PyIceberg REST catalog '%s'...", polaris.catalog)

    catalog = RestCatalog(
        name=polaris.catalog,
        **{
            "uri": polaris.uri,
            "warehouse": polaris.catalog,
            "credential": f"{polaris.client_id}:{polaris.client_secret}",
            "scope": "PRINCIPAL_ROLE:ALL",
        },
    )

    logger.info("Catalog initialized successfully")

    return catalog
