"""Dagster sensors for monitoring and auto-recovery of streaming jobs."""

import json
import time
from typing import Any

import dagster as dg
from dagster import EnvVar


# Module-level state to track last restart timestamp (persists within process)
_last_restart_timestamps: dict[str, float] = {}


@dg.run_failure_sensor(
    description="Monitors bronze_streaming_job failures and auto-restarts with backoff",
)
def bronze_restart_sensor(
    context: dg.RunFailureSensorContext,
) -> dg.RunRequest | dg.SkipReason:
    """Auto-restart bronze_streaming_job on failure with backoff.

    Uses module-level state to track last restart timestamp to prevent restart loops.
    Respects STREAMING_RESTART_MIN_INTERVAL_SECONDS environment variable (default: 60s).
    """
    # Only monitor bronze_streaming_job
    if context.dagster_run.job_name != "bronze_streaming_job":
        return dg.SkipReason(f"Skipping non-bronze job: {context.dagster_run.job_name}")

    # Get the restart interval from environment variable
    restart_min_interval_seconds = int(
        EnvVar("STREAMING_RESTART_MIN_INTERVAL_SECONDS").get_value() or "60"
    )

    # Log the failure event message
    context.log.warning(f"bronze_streaming_job failed: {context.failure_event.message}")

    # Get last restart timestamp for this job
    job_name = context.dagster_run.job_name
    last_restart_ts = _last_restart_timestamps.get(job_name, 0.0)
    current_ts = time.time()

    # Check if enough time has elapsed since last restart
    elapsed_seconds = current_ts - last_restart_ts
    if elapsed_seconds < restart_min_interval_seconds:
        context.log.info(
            f"Skipping restart - only {elapsed_seconds:.1f}s elapsed, "
            f"need {restart_min_interval_seconds}s cooldown"
        )
        return dg.SkipReason(
            f"Cooldown active: {elapsed_seconds:.1f}s < {restart_min_interval_seconds}s"
        )

    # Update last restart timestamp
    _last_restart_timestamps[job_name] = current_ts

    context.log.info(
        f"Triggering restart of bronze_streaming_job after {elapsed_seconds:.1f}s"
    )

    return dg.RunRequest(
        run_key=f"bronze_restart_{current_ts}",
        tags={
            "triggered_by": "bronze_restart_sensor",
            "failure_run_id": context.dagster_run.run_id,
        },
    )


@dg.sensor(
    description="Monitors Kafka consumer group lag for all streaming topics",
    minimum_interval_seconds=60,
)
def kafka_lag_sensor(
    context: dg.SensorEvaluationContext,
) -> dg.SensorResult:
    """Poll Kafka consumer group lag and emit metadata.

    Checks lag for all consumer groups reading from the three streaming topics.
    Emits a warning log when total lag exceeds KAFKA_LAG_WARN_THRESHOLD (default: 10000).
    Stores last-checked timestamp in cursor.
    """
    # Get configuration from environment FIRST (before importing kafka)
    bootstrap_servers = EnvVar("KAFKA_BOOTSTRAP_SERVERS").get_value()
    lag_threshold = int(EnvVar("KAFKA_LAG_WARN_THRESHOLD").get_value() or "10000")

    if not bootstrap_servers:
        context.log.warning("KAFKA_BOOTSTRAP_SERVERS not set, skipping lag check")
        return dg.SensorResult()

    # Lazy import to avoid kafka-python import issues at definition load time
    from kafka import KafkaAdminClient, KafkaConsumer
    from kafka.structs import TopicPartition

    if not bootstrap_servers:
        context.log.warning("KAFKA_BOOTSTRAP_SERVERS not set, skipping lag check")
        return dg.SensorResult()

    # Topics to monitor
    topics = [
        "listen_events",
        "page_view_events",
        "auth_events",
    ]

    lag_data: dict[str, Any] = {}
    total_lag_by_topic: dict[str, int] = {}

    admin_client = None
    consumer = None

    try:
        # Connect to Kafka
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

        # Create a consumer to get end offsets
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id="lag-monitor-sensor",
            enable_auto_commit=False,
        )

        # Get all consumer groups
        consumer_groups = admin_client.list_consumer_groups()
        group_ids = [group[0] for group in consumer_groups]

        context.log.info(f"Checking lag for consumer groups: {group_ids}")

        for topic in topics:
            topic_lag = 0
            partition_lags: list[dict[str, Any]] = []

            # Get topic partitions
            try:
                topic_partitions_info = admin_client.describe_topics([topic])
                if topic_partitions_info:
                    partitions = topic_partitions_info[0].get("partitions", [])
                    partition_ids = [p["partition"] for p in partitions]
                else:
                    context.log.warning(f"Topic {topic} not found")
                    continue
            except Exception as e:
                context.log.warning(f"Error describing topic {topic}: {e}")
                continue

            # Create TopicPartition objects
            tps = [TopicPartition(topic, p) for p in partition_ids]

            # Get end offsets (high watermark) using consumer
            end_offsets = consumer.end_offsets(tps)

            for group_id in group_ids:
                try:
                    # Get committed offsets for this group
                    committed_offsets = admin_client.list_consumer_group_offsets(
                        group_id, [topic]
                    )

                    # Calculate lag per partition
                    for partition_id in partition_ids:
                        tp = TopicPartition(topic, partition_id)

                        # Get committed offset for this partition
                        committed = committed_offsets.get(tp, 0)
                        # Get end offset for this partition
                        end_offset = end_offsets.get(tp, 0) if end_offsets else 0

                        partition_lag = max(0, end_offset - committed)
                        topic_lag += partition_lag

                        if partition_lag > 0 or committed > 0:
                            partition_lags.append(
                                {
                                    "partition": partition_id,
                                    "consumer_group": group_id,
                                    "committed_offset": committed,
                                    "end_offset": end_offset,
                                    "lag": partition_lag,
                                }
                            )

                except Exception as e:
                    context.log.warning(
                        f"Error checking lag for group {group_id} on topic {topic}: {e}"
                    )

            lag_data[topic] = {
                "total_lag": topic_lag,
                "partitions": partition_lags,
            }
            total_lag_by_topic[topic] = topic_lag

            # Check threshold and log warning
            if topic_lag > lag_threshold:
                context.log.warning(
                    f"Kafka lag for topic '{topic}' ({topic_lag}) exceeds threshold "
                    f"({lag_threshold})"
                )

        # Clean up
        if consumer:
            consumer.close()
        if admin_client:
            admin_client.close()

    except Exception as e:
        context.log.error(f"Failed to check Kafka lag: {e}")
        # Clean up on error
        if consumer:
            consumer.close()
        if admin_client:
            admin_client.close()
        # Return partial result even on error
        return dg.SensorResult(
            cursor=json.dumps(
                {
                    "last_checked": time.time(),
                    "error": str(e),
                }
            )
        )

    # Update cursor with last-checked timestamp
    new_cursor = json.dumps(
        {
            "last_checked": time.time(),
            "topics_checked": topics,
        }
    )

    context.log.info(
        f"Kafka lag check complete. Total lag by topic: {total_lag_by_topic}"
    )

    return dg.SensorResult(
        cursor=new_cursor,
        dagster_run_requests=[],
    )
