"""Dagster sensors for monitoring and auto-recovery of streaming jobs."""

import time

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
