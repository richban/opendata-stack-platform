"""Dagster assets for managing Spark Structured Streaming jobs via Pipes.

This module uses Dagster Pipes to launch and monitor long-running Spark streaming jobs.
The streaming job runs via spark-submit and communicates back to Dagster via S3.

Configuration is passed via StreamingJobConfig resource and Pipes extras.
"""

import os
import subprocess
from pathlib import Path

import dagster as dg
from dagster_aws.pipes import PipesS3ContextInjector, PipesS3MessageReader
from dagster_aws.s3 import S3Resource

from team_ops.defs.resources import StreamingJobConfig

SCRIPT_PATH = Path(__file__).parent.parent / "spark_scripts" / "stream_to_iceberg.py"


@dg.asset(
    group_name="streaming",
    compute_kind="spark-streaming",
    description="Kafka to Iceberg Bronze streaming job (via Dagster Pipes)",
)
def bronze_streaming_job(
    context: dg.AssetExecutionContext,
    s3: S3Resource,
    streaming_config: StreamingJobConfig,
):
    """Launch Spark Structured Streaming job to ingest Kafka events to Iceberg Bronze.

    This asset uses Dagster Pipes to:
    1. Upload the PySpark script to S3
    2. Launch spark-submit with the script
    3. Monitor the streaming job via S3 message passing
    4. Collect logs and metadata from the Spark driver

    The streaming job runs continuously and this asset represents its lifecycle.

    Configuration is provided via StreamingJobConfig resource and passed to
    the streaming script via Pipes extras (no argparse in the script!).

    Note: This is a long-running streaming job. In production, consider:
    - Running with a timeout or max duration
    - Using a separate scheduler/cron to restart if needed
    - Monitoring with sensors for health checks
    """
    s3_client = s3.get_client()

    bucket = streaming_config.dagster_pipes_bucket
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

    with dg.open_pipes_session(
        context=context,
        message_reader=message_reader,
        context_injector=context_injector,
        extras={
            "kafka_bootstrap_servers": streaming_config.kafka_bootstrap_servers,
            "checkpoint_path": streaming_config.checkpoint_path,
            "polaris_uri": streaming_config.polaris_uri,
            "polaris_credential": streaming_config.get_polaris_credential(),
            "catalog": streaming_config.catalog,
            "namespace": streaming_config.namespace,
        },
    ) as session:
        bootstrap_env_vars = session.get_bootstrap_env_vars()

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
            f"spark.hadoop.fs.s3a.endpoint={s3.endpoint_url}",
            "--conf",
            "spark.hadoop.fs.s3a.path.style.access=true",
            "--conf",
            f"spark.hadoop.fs.s3a.access.key={s3.aws_access_key_id}",
            "--conf",
            f"spark.hadoop.fs.s3a.secret.key={s3.aws_secret_access_key}",
            f"s3a://{bucket}/{s3_script_path}",
        ]

        context.log.info(f"Launching spark-submit with {len(cmd)} arguments")
        context.log.info(f"Bootstrap env vars: {list(bootstrap_env_vars.keys())}")
        context.log.info(
            f"Configuration: catalog={streaming_config.catalog}.{streaming_config.namespace}"
        )

        process = subprocess.Popen(
            cmd,
            env={**os.environ.copy(), **bootstrap_env_vars},
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        try:
            while process.poll() is None:
                yield from session.get_results()

            if process.returncode != 0:
                stdout, stderr = process.communicate()
                raise RuntimeError(
                    f"Spark job failed with return code {process.returncode}\n"
                    f"STDOUT: {stdout.decode() if stdout else '<empty>'}\n"
                    f"STDERR: {stderr.decode() if stderr else '<empty>'}"
                )

        except Exception:
            context.log.error("Terminating Spark job due to error")
            process.terminate()
            raise

    yield from session.get_results()
