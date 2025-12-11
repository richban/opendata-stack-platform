"""Dagster assets for the Streamify lakehouse pipeline.

This module defines assets for the Silver layer batch processing:
- Deduplication of Bronze events
- Sessionization of user activity
- Table compaction (maintenance)
- Snapshot expiry (maintenance)

Assets use Dagster Pipes to submit Spark jobs and receive metadata.
"""

from __future__ import annotations

import os
import subprocess
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import boto3
import dagster as dg
from dagster_aws.pipes import PipesS3ContextInjector, PipesS3MessageReader


def get_spark_script_path(script_name: str) -> str:
    """Get the path to a Spark script."""
    return str(Path(__file__).parent.parent / "spark_scripts" / script_name)


def get_s3_client() -> Any:
    """Create an S3 client for MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "miniouser"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "miniouser"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )


def build_spark_submit_cmd(
    script_path: str,
    packages: str | None = None,
    extra_args: list[str] | None = None,
) -> list[str]:
    """Build a spark-submit command."""
    cmd = [
        "spark-submit",
        "--master",
        os.getenv("SPARK_MASTER", "spark://spark-master:7077"),
        "--deploy-mode",
        "client",
    ]

    if packages:
        cmd.extend(["--packages", packages])

    cmd.append(script_path)

    if extra_args:
        cmd.extend(extra_args)

    return cmd


# Default packages for Iceberg
ICEBERG_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1,"
    "org.apache.iceberg:iceberg-aws-bundle:1.7.1"
)


class StreamifyConfig(dg.Config):
    """Configuration for Streamify assets."""

    polaris_uri: str = "http://polaris:8181/api/catalog"
    polaris_credential: str = ""
    catalog: str = "lakehouse"
    namespace: str = "streamify"


class DailyPartitionConfig(dg.Config):
    """Configuration with date partition."""

    polaris_uri: str = "http://polaris:8181/api/catalog"
    polaris_credential: str = ""
    catalog: str = "lakehouse"
    namespace: str = "streamify"
    event_date: str = ""

    def __init__(self, **data):
        if "event_date" not in data or not data["event_date"]:
            data["event_date"] = (date.today() - timedelta(days=1)).isoformat()
        super().__init__(**data)


# =============================================================================
# Silver Layer: Deduplication Assets
# =============================================================================


@dg.asset(
    group_name="silver",
    compute_kind="spark",
    description="Deduplicated listen events from Bronze layer",
    metadata={
        "table": "lakehouse.streamify.silver_listen_events",
        "layer": "silver",
    },
)
def silver_listen_events(
    context: dg.AssetExecutionContext,
    config: DailyPartitionConfig,
) -> dg.MaterializeResult:
    """Deduplicate listen_events from Bronze to Silver."""
    return _run_deduplication(context, config, "listen_events")


@dg.asset(
    group_name="silver",
    compute_kind="spark",
    description="Deduplicated page view events from Bronze layer",
    metadata={
        "table": "lakehouse.streamify.silver_page_view_events",
        "layer": "silver",
    },
)
def silver_page_view_events(
    context: dg.AssetExecutionContext,
    config: DailyPartitionConfig,
) -> dg.MaterializeResult:
    """Deduplicate page_view_events from Bronze to Silver."""
    return _run_deduplication(context, config, "page_view_events")


@dg.asset(
    group_name="silver",
    compute_kind="spark",
    description="Deduplicated auth events from Bronze layer",
    metadata={
        "table": "lakehouse.streamify.silver_auth_events",
        "layer": "silver",
    },
)
def silver_auth_events(
    context: dg.AssetExecutionContext,
    config: DailyPartitionConfig,
) -> dg.MaterializeResult:
    """Deduplicate auth_events from Bronze to Silver."""
    return _run_deduplication(context, config, "auth_events")


def _run_deduplication(
    context: dg.AssetExecutionContext,
    config: DailyPartitionConfig,
    topic: str,
) -> dg.MaterializeResult:
    """Run deduplication Spark job with Pipes."""
    script_path = get_spark_script_path("deduplicate_events.py")
    s3_client = get_s3_client()
    bucket = os.getenv("DAGSTER_PIPES_BUCKET", "dagster-pipes")

    context_injector = PipesS3ContextInjector(client=s3_client, bucket=bucket)
    message_reader = PipesS3MessageReader(
        client=s3_client,
        bucket=bucket,
        include_stdio_in_messages=True,
    )

    extra_args = [
        "--polaris-uri",
        config.polaris_uri,
        "--polaris-credential",
        config.polaris_credential,
        "--catalog",
        config.catalog,
        "--namespace",
        config.namespace,
        "--event-date",
        config.event_date,
        "--topic",
        topic,
    ]

    with dg.open_pipes_session(
        context=context,
        message_reader=message_reader,
        context_injector=context_injector,
    ) as session:
        # Get Pipes bootstrap args
        pipes_args = [
            f"{key}={value}"
            for key, value in session.get_bootstrap_cli_arguments().items()
        ]
        extra_args.extend(pipes_args)

        cmd = build_spark_submit_cmd(
            script_path,
            packages=ICEBERG_PACKAGES,
            extra_args=extra_args,
        )

        context.log.info(f"Running: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode != 0:
            context.log.error(f"Spark job failed: {result.stderr[-2000:]}")
            raise Exception(f"Deduplication failed for {topic}")

    return session.get_results()


# =============================================================================
# Silver Layer: Sessionization Asset
# =============================================================================


@dg.asset(
    group_name="silver",
    compute_kind="spark",
    deps=[silver_listen_events, silver_page_view_events, silver_auth_events],
    description="Sessionized user activity combining all event types",
    metadata={
        "table": "lakehouse.streamify.silver_user_sessions",
        "layer": "silver",
        "session_timeout_minutes": 30,
    },
)
def silver_user_sessions(
    context: dg.AssetExecutionContext,
    config: DailyPartitionConfig,
) -> dg.MaterializeResult:
    """Build user sessions from deduplicated Silver events."""
    script_path = get_spark_script_path("sessionize_events.py")
    s3_client = get_s3_client()
    bucket = os.getenv("DAGSTER_PIPES_BUCKET", "dagster-pipes")

    context_injector = PipesS3ContextInjector(client=s3_client, bucket=bucket)
    message_reader = PipesS3MessageReader(
        client=s3_client,
        bucket=bucket,
        include_stdio_in_messages=True,
    )

    extra_args = [
        "--polaris-uri",
        config.polaris_uri,
        "--polaris-credential",
        config.polaris_credential,
        "--catalog",
        config.catalog,
        "--namespace",
        config.namespace,
        "--event-date",
        config.event_date,
    ]

    with dg.open_pipes_session(
        context=context,
        message_reader=message_reader,
        context_injector=context_injector,
    ) as session:
        pipes_args = [
            f"{key}={value}"
            for key, value in session.get_bootstrap_cli_arguments().items()
        ]
        extra_args.extend(pipes_args)

        cmd = build_spark_submit_cmd(
            script_path,
            packages=ICEBERG_PACKAGES,
            extra_args=extra_args,
        )

        context.log.info(f"Running: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode != 0:
            context.log.error(f"Spark job failed: {result.stderr[-2000:]}")
            raise Exception("Sessionization failed")

    return session.get_results()


# =============================================================================
# Maintenance Assets
# =============================================================================


@dg.asset(
    group_name="maintenance",
    compute_kind="spark",
    description="Compact Iceberg tables to merge small files",
    metadata={
        "target_file_size_mb": 128,
    },
)
def compact_tables(
    context: dg.AssetExecutionContext,
    config: StreamifyConfig,
) -> dg.MaterializeResult:
    """Run Iceberg table compaction."""
    script_path = get_spark_script_path("compact_tables.py")
    s3_client = get_s3_client()
    bucket = os.getenv("DAGSTER_PIPES_BUCKET", "dagster-pipes")

    context_injector = PipesS3ContextInjector(client=s3_client, bucket=bucket)
    message_reader = PipesS3MessageReader(
        client=s3_client,
        bucket=bucket,
        include_stdio_in_messages=True,
    )

    extra_args = [
        "--polaris-uri",
        config.polaris_uri,
        "--polaris-credential",
        config.polaris_credential,
        "--catalog",
        config.catalog,
        "--namespace",
        config.namespace,
    ]

    with dg.open_pipes_session(
        context=context,
        message_reader=message_reader,
        context_injector=context_injector,
    ) as session:
        pipes_args = [
            f"{key}={value}"
            for key, value in session.get_bootstrap_cli_arguments().items()
        ]
        extra_args.extend(pipes_args)

        cmd = build_spark_submit_cmd(
            script_path,
            packages=ICEBERG_PACKAGES,
            extra_args=extra_args,
        )

        context.log.info(f"Running: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode != 0:
            context.log.error(f"Spark job failed: {result.stderr[-2000:]}")
            raise Exception("Compaction failed")

    return session.get_results()


@dg.asset(
    group_name="maintenance",
    compute_kind="spark",
    description="Expire old Iceberg snapshots (7-day retention)",
    metadata={
        "retention_days": 7,
        "retain_last_snapshots": 10,
    },
)
def expire_snapshots(
    context: dg.AssetExecutionContext,
    config: StreamifyConfig,
) -> dg.MaterializeResult:
    """Expire old Iceberg snapshots."""
    script_path = get_spark_script_path("expire_snapshots.py")
    s3_client = get_s3_client()
    bucket = os.getenv("DAGSTER_PIPES_BUCKET", "dagster-pipes")

    context_injector = PipesS3ContextInjector(client=s3_client, bucket=bucket)
    message_reader = PipesS3MessageReader(
        client=s3_client,
        bucket=bucket,
        include_stdio_in_messages=True,
    )

    extra_args = [
        "--polaris-uri",
        config.polaris_uri,
        "--polaris-credential",
        config.polaris_credential,
        "--catalog",
        config.catalog,
        "--namespace",
        config.namespace,
        "--retention-days",
        "7",
    ]

    with dg.open_pipes_session(
        context=context,
        message_reader=message_reader,
        context_injector=context_injector,
    ) as session:
        pipes_args = [
            f"{key}={value}"
            for key, value in session.get_bootstrap_cli_arguments().items()
        ]
        extra_args.extend(pipes_args)

        cmd = build_spark_submit_cmd(
            script_path,
            packages=ICEBERG_PACKAGES,
            extra_args=extra_args,
        )

        context.log.info(f"Running: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode != 0:
            context.log.error(f"Spark job failed: {result.stderr[-2000:]}")
            raise Exception("Snapshot expiry failed")

    return session.get_results()
