"""Dagster assets for managing Spark Structured Streaming jobs via Pipes.

This module uses Dagster Pipes to launch and monitor long-running Spark streaming jobs.
The streaming job runs via spark-submit and communicates back to Dagster via S3.
"""

import os
import subprocess
from pathlib import Path

import boto3
import dagster as dg
from dagster_aws.pipes import PipesS3ContextInjector, PipesS3MessageReader

SCRIPT_PATH = Path(__file__).parent.parent / "spark_scripts" / "stream_to_iceberg.py"


@dg.asset(
    group_name="streaming",
    compute_kind="spark-streaming",
    description="Kafka to Iceberg Bronze streaming job (via Dagster Pipes)",
)
def bronze_streaming_job(context: dg.AssetExecutionContext):
    """Launch Spark Structured Streaming job to ingest Kafka events to Iceberg Bronze.

    This asset uses Dagster Pipes to:
    1. Upload the PySpark script to S3
    2. Launch spark-submit with the script
    3. Monitor the streaming job via S3 message passing
    4. Collect logs and metadata from the Spark driver

    The streaming job runs continuously and this asset represents its lifecycle.
    """
    s3_client = boto3.client(
        "s3",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )

    bucket = os.environ["DAGSTER_PIPES_BUCKET"]
    s3_script_path = f"{context.dagster_run.run_id}/stream_to_iceberg.py"

    context.log.info(f"Uploading streaming script to s3://{bucket}/{s3_script_path}")
    s3_client.upload_file(str(SCRIPT_PATH), bucket, s3_script_path)

    context_injector = PipesS3ContextInjector(
        client=s3_client,
        bucket=bucket,
    )

    message_reader = PipesS3MessageReader(
        client=s3_client,
        bucket=bucket,
        include_stdio_in_messages=True,
    )

    polaris_client_id = os.environ["POLARIS_CLIENT_ID"]
    polaris_client_secret = os.environ["POLARIS_CLIENT_SECRET"]
    polaris_credential = f"{polaris_client_id}:{polaris_client_secret}"

    with dg.open_pipes_session(
        context=context,
        message_reader=message_reader,
        context_injector=context_injector,
    ) as session:
        dagster_pipes_args = " ".join(
            [
                f"{key} {value}"
                for key, value in session.get_bootstrap_cli_arguments().items()
            ]
        )

        cmd = [
            "spark-submit",
            "--master",
            "spark://spark-master:7077",
            "--deploy-mode",
            "client",
            "--packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1,"
            "org.apache.iceberg:iceberg-aws-bundle:1.7.1",
            "--conf",
            "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
            "--conf",
            f"spark.hadoop.fs.s3a.endpoint={os.environ.get('AWS_ENDPOINT_URL', 'http://minio:9000')}",
            "--conf",
            "spark.hadoop.fs.s3a.path.style.access=true",
            "--conf",
            f"spark.hadoop.fs.s3a.access.key={os.environ.get('AWS_ACCESS_KEY_ID', 'minio')}",
            "--conf",
            f"spark.hadoop.fs.s3a.secret.key={os.environ.get('AWS_SECRET_ACCESS_KEY', 'minio123')}",
            f"s3a://{bucket}/{s3_script_path}",
            "--kafka-bootstrap-servers",
            "kafka:9092",
            "--polaris-credential",
            polaris_credential,
            "--checkpoint-path",
            "s3a://checkpoints/streaming",
        ]

        cmd_with_pipes = cmd + dagster_pipes_args.split()

        context.log.info(f"Launching spark-submit: {' '.join(cmd[:5])}...")

        subprocess.run(
            cmd_with_pipes,
            check=True,
        )

    return session.get_results()
