"""Data Quality checks for Silver layer assets.

This module contains Dagster asset checks that validate data quality
for Silver layer assets. Check results are persisted to a local SQLite
database and logged to a file for traceability.
"""

import os

from typing import Any

import dagster as dg

from pyspark.sql.functions import col

from team_ops.defs.dq_store import DQResultStore
from team_ops.defs.resources import SparkConnectResource, StreamingJobConfig


def _get_dq_store() -> DQResultStore:
    """Get DQResultStore instance with paths from environment or defaults."""
    db_path = os.getenv("DQ_RESULTS_DB_PATH", "dq_results/dq_checks.db")
    log_path = os.getenv("DQ_RESULTS_LOG_PATH", "dq_results/dq_checks.log")
    return DQResultStore(db_path=db_path, log_path=log_path)


def _write_check_result(
    store: DQResultStore,
    check_name: str,
    asset_name: str,
    passed: bool,
    context: dg.AssetCheckExecutionContext,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Write check result to store and log."""
    run_id_val = getattr(context, "run_id", None)
    run_id = str(run_id_val) if run_id_val is not None else None
    store.write_result(
        check_name=check_name,
        asset_name=asset_name,
        passed=passed,
        run_id=run_id,
        metadata=metadata,
    )


# =============================================================================
# silver_listen_events Checks
# =============================================================================


@dg.asset_check(asset="silver_listen_events")
def silver_listen_events_not_empty(
    context: dg.AssetCheckExecutionContext,
    spark: SparkConnectResource,
    streaming_config: StreamingJobConfig,
) -> dg.AssetCheckResult:
    """Check that silver_listen_events is not empty."""
    store = _get_dq_store()
    asset_name = "silver_listen_events"
    check_name = "not_empty"

    session = spark.get_session()
    catalog = streaming_config.catalog
    namespace = streaming_config.namespace
    table_name = f"{catalog}.{namespace}.silver_listen_events"

    try:
        df = session.table(table_name)
        row_count = df.count()
        passed = row_count > 0

        metadata: dict[str, Any] = {"row_count": row_count}
        _write_check_result(store, check_name, asset_name, passed, context, metadata)

        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
            },
        )
    except Exception as e:
        _write_check_result(
            store, check_name, asset_name, False, context, {"error": str(e)}
        )
        return dg.AssetCheckResult(
            passed=False,
            metadata={"error": dg.MetadataValue.text(str(e))},
        )


@dg.asset_check(asset="silver_listen_events")
def silver_listen_events_no_nulls(
    context: dg.AssetCheckExecutionContext,
    spark: SparkConnectResource,
    streaming_config: StreamingJobConfig,
) -> dg.AssetCheckResult:
    """Check that null event_id and userId are below threshold (0.01%)."""
    store = _get_dq_store()
    asset_name = "silver_listen_events"
    check_name = "no_nulls"
    threshold = 0.0001  # 0.01%

    session = spark.get_session()
    catalog = streaming_config.catalog
    namespace = streaming_config.namespace
    table_name = f"{catalog}.{namespace}.silver_listen_events"

    try:
        df = session.table(table_name)
        total_rows = df.count()

        if total_rows == 0:
            metadata: dict[str, Any] = {
                "total_rows": 0,
                "null_event_id": 0,
                "null_user_id": 0,
            }
            _write_check_result(store, check_name, asset_name, True, context, metadata)
            return dg.AssetCheckResult(
                passed=True,
                metadata={"total_rows": dg.MetadataValue.int(0)},
            )

        # Count nulls in event_id and userId
        null_event_id = df.filter(col("event_id").isNull()).count()
        null_user_id = df.filter(col("userId").isNull()).count()

        event_id_null_pct = null_event_id / total_rows
        user_id_null_pct = null_user_id / total_rows

        passed = event_id_null_pct <= threshold and user_id_null_pct <= threshold

        metadata = {
            "total_rows": total_rows,
            "null_event_id": null_event_id,
            "null_user_id": null_user_id,
            "event_id_null_pct": event_id_null_pct,
            "user_id_null_pct": user_id_null_pct,
            "threshold": threshold,
        }
        _write_check_result(store, check_name, asset_name, passed, context, metadata)

        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "total_rows": dg.MetadataValue.int(total_rows),
                "null_event_id": dg.MetadataValue.int(null_event_id),
                "null_user_id": dg.MetadataValue.int(null_user_id),
                "threshold_pct": dg.MetadataValue.float(threshold * 100),
            },
        )
    except Exception as e:
        _write_check_result(
            store, check_name, asset_name, False, context, {"error": str(e)}
        )
        return dg.AssetCheckResult(
            passed=False,
            metadata={"error": dg.MetadataValue.text(str(e))},
        )


@dg.asset_check(asset="silver_listen_events")
def silver_listen_events_no_duplicates(
    context: dg.AssetCheckExecutionContext,
    spark: SparkConnectResource,
    streaming_config: StreamingJobConfig,
) -> dg.AssetCheckResult:
    """Check that there are no duplicate event_ids."""
    store = _get_dq_store()
    asset_name = "silver_listen_events"
    check_name = "no_duplicates"

    session = spark.get_session()
    catalog = streaming_config.catalog
    namespace = streaming_config.namespace
    table_name = f"{catalog}.{namespace}.silver_listen_events"

    try:
        df = session.table(table_name)
        total_rows = df.count()

        if total_rows == 0:
            metadata: dict[str, Any] = {
                "total_rows": 0,
                "distinct_event_ids": 0,
                "duplicates": 0,
            }
            _write_check_result(store, check_name, asset_name, True, context, metadata)
            return dg.AssetCheckResult(
                passed=True,
                metadata={"total_rows": dg.MetadataValue.int(0)},
            )

        distinct_event_ids = df.select("event_id").distinct().count()
        duplicates = total_rows - distinct_event_ids
        passed = duplicates == 0

        metadata = {
            "total_rows": total_rows,
            "distinct_event_ids": distinct_event_ids,
            "duplicates": duplicates,
        }
        _write_check_result(store, check_name, asset_name, passed, context, metadata)

        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "total_rows": dg.MetadataValue.int(total_rows),
                "distinct_event_ids": dg.MetadataValue.int(distinct_event_ids),
                "duplicates": dg.MetadataValue.int(duplicates),
            },
        )
    except Exception as e:
        _write_check_result(
            store, check_name, asset_name, False, context, {"error": str(e)}
        )
        return dg.AssetCheckResult(
            passed=False,
            metadata={"error": dg.MetadataValue.text(str(e))},
        )


# =============================================================================
# silver_page_view_events Checks
# =============================================================================


@dg.asset_check(asset="silver_page_view_events")
def silver_page_view_events_not_empty(
    context: dg.AssetCheckExecutionContext,
    spark: SparkConnectResource,
    streaming_config: StreamingJobConfig,
) -> dg.AssetCheckResult:
    """Check that silver_page_view_events is not empty."""
    store = _get_dq_store()
    asset_name = "silver_page_view_events"
    check_name = "not_empty"

    session = spark.get_session()
    catalog = streaming_config.catalog
    namespace = streaming_config.namespace
    table_name = f"{catalog}.{namespace}.silver_page_view_events"

    try:
        df = session.table(table_name)
        row_count = df.count()
        passed = row_count > 0

        metadata: dict[str, Any] = {"row_count": row_count}
        _write_check_result(store, check_name, asset_name, passed, context, metadata)

        return dg.AssetCheckResult(
            passed=passed,
            metadata={"row_count": dg.MetadataValue.int(row_count)},
        )
    except Exception as e:
        _write_check_result(
            store, check_name, asset_name, False, context, {"error": str(e)}
        )
        return dg.AssetCheckResult(
            passed=False,
            metadata={"error": dg.MetadataValue.text(str(e))},
        )


@dg.asset_check(asset="silver_page_view_events")
def silver_page_view_events_no_nulls(
    context: dg.AssetCheckExecutionContext,
    spark: SparkConnectResource,
    streaming_config: StreamingJobConfig,
) -> dg.AssetCheckResult:
    """Check that null event_id and userId are below threshold (0.01%)."""
    store = _get_dq_store()
    asset_name = "silver_page_view_events"
    check_name = "no_nulls"
    threshold = 0.0001  # 0.01%

    session = spark.get_session()
    catalog = streaming_config.catalog
    namespace = streaming_config.namespace
    table_name = f"{catalog}.{namespace}.silver_page_view_events"

    try:
        df = session.table(table_name)
        total_rows = df.count()

        if total_rows == 0:
            metadata: dict[str, Any] = {
                "total_rows": 0,
                "null_event_id": 0,
                "null_user_id": 0,
            }
            _write_check_result(store, check_name, asset_name, True, context, metadata)
            return dg.AssetCheckResult(
                passed=True,
                metadata={"total_rows": dg.MetadataValue.int(0)},
            )

        null_event_id = df.filter(col("event_id").isNull()).count()
        null_user_id = df.filter(col("userId").isNull()).count()

        event_id_null_pct = null_event_id / total_rows
        user_id_null_pct = null_user_id / total_rows

        passed = event_id_null_pct <= threshold and user_id_null_pct <= threshold

        metadata = {
            "total_rows": total_rows,
            "null_event_id": null_event_id,
            "null_user_id": null_user_id,
            "event_id_null_pct": event_id_null_pct,
            "user_id_null_pct": user_id_null_pct,
            "threshold": threshold,
        }
        _write_check_result(store, check_name, asset_name, passed, context, metadata)

        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "total_rows": dg.MetadataValue.int(total_rows),
                "null_event_id": dg.MetadataValue.int(null_event_id),
                "null_user_id": dg.MetadataValue.int(null_user_id),
                "threshold_pct": dg.MetadataValue.float(threshold * 100),
            },
        )
    except Exception as e:
        _write_check_result(
            store, check_name, asset_name, False, context, {"error": str(e)}
        )
        return dg.AssetCheckResult(
            passed=False,
            metadata={"error": dg.MetadataValue.text(str(e))},
        )


@dg.asset_check(asset="silver_page_view_events")
def silver_page_view_events_no_duplicates(
    context: dg.AssetCheckExecutionContext,
    spark: SparkConnectResource,
    streaming_config: StreamingJobConfig,
) -> dg.AssetCheckResult:
    """Check that there are no duplicate event_ids."""
    store = _get_dq_store()
    asset_name = "silver_page_view_events"
    check_name = "no_duplicates"

    session = spark.get_session()
    catalog = streaming_config.catalog
    namespace = streaming_config.namespace
    table_name = f"{catalog}.{namespace}.silver_page_view_events"

    try:
        df = session.table(table_name)
        total_rows = df.count()

        if total_rows == 0:
            metadata: dict[str, Any] = {
                "total_rows": 0,
                "distinct_event_ids": 0,
                "duplicates": 0,
            }
            _write_check_result(store, check_name, asset_name, True, context, metadata)
            return dg.AssetCheckResult(
                passed=True,
                metadata={"total_rows": dg.MetadataValue.int(0)},
            )

        distinct_event_ids = df.select("event_id").distinct().count()
        duplicates = total_rows - distinct_event_ids
        passed = duplicates == 0

        metadata = {
            "total_rows": total_rows,
            "distinct_event_ids": distinct_event_ids,
            "duplicates": duplicates,
        }
        _write_check_result(store, check_name, asset_name, passed, context, metadata)

        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "total_rows": dg.MetadataValue.int(total_rows),
                "distinct_event_ids": dg.MetadataValue.int(distinct_event_ids),
                "duplicates": dg.MetadataValue.int(duplicates),
            },
        )
    except Exception as e:
        _write_check_result(
            store, check_name, asset_name, False, context, {"error": str(e)}
        )
        return dg.AssetCheckResult(
            passed=False,
            metadata={"error": dg.MetadataValue.text(str(e))},
        )


# =============================================================================
# silver_auth_events Checks
# =============================================================================


@dg.asset_check(asset="silver_auth_events")
def silver_auth_events_not_empty(
    context: dg.AssetCheckExecutionContext,
    spark: SparkConnectResource,
    streaming_config: StreamingJobConfig,
) -> dg.AssetCheckResult:
    """Check that silver_auth_events is not empty."""
    store = _get_dq_store()
    asset_name = "silver_auth_events"
    check_name = "not_empty"

    session = spark.get_session()
    catalog = streaming_config.catalog
    namespace = streaming_config.namespace
    table_name = f"{catalog}.{namespace}.silver_auth_events"

    try:
        df = session.table(table_name)
        row_count = df.count()
        passed = row_count > 0

        metadata: dict[str, Any] = {"row_count": row_count}
        _write_check_result(store, check_name, asset_name, passed, context, metadata)

        return dg.AssetCheckResult(
            passed=passed,
            metadata={"row_count": dg.MetadataValue.int(row_count)},
        )
    except Exception as e:
        _write_check_result(
            store, check_name, asset_name, False, context, {"error": str(e)}
        )
        return dg.AssetCheckResult(
            passed=False,
            metadata={"error": dg.MetadataValue.text(str(e))},
        )


@dg.asset_check(asset="silver_auth_events")
def silver_auth_events_no_nulls(
    context: dg.AssetCheckExecutionContext,
    spark: SparkConnectResource,
    streaming_config: StreamingJobConfig,
) -> dg.AssetCheckResult:
    """Check that null event_id and userId are below threshold (0.01%)."""
    store = _get_dq_store()
    asset_name = "silver_auth_events"
    check_name = "no_nulls"
    threshold = 0.0001  # 0.01%

    session = spark.get_session()
    catalog = streaming_config.catalog
    namespace = streaming_config.namespace
    table_name = f"{catalog}.{namespace}.silver_auth_events"

    try:
        df = session.table(table_name)
        total_rows = df.count()

        if total_rows == 0:
            metadata: dict[str, Any] = {
                "total_rows": 0,
                "null_event_id": 0,
                "null_user_id": 0,
            }
            _write_check_result(store, check_name, asset_name, True, context, metadata)
            return dg.AssetCheckResult(
                passed=True,
                metadata={"total_rows": dg.MetadataValue.int(0)},
            )

        null_event_id = df.filter(col("event_id").isNull()).count()
        null_user_id = df.filter(col("userId").isNull()).count()

        event_id_null_pct = null_event_id / total_rows
        user_id_null_pct = null_user_id / total_rows

        passed = event_id_null_pct <= threshold and user_id_null_pct <= threshold

        metadata = {
            "total_rows": total_rows,
            "null_event_id": null_event_id,
            "null_user_id": null_user_id,
            "event_id_null_pct": event_id_null_pct,
            "user_id_null_pct": user_id_null_pct,
            "threshold": threshold,
        }
        _write_check_result(store, check_name, asset_name, passed, context, metadata)

        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "total_rows": dg.MetadataValue.int(total_rows),
                "null_event_id": dg.MetadataValue.int(null_event_id),
                "null_user_id": dg.MetadataValue.int(null_user_id),
                "threshold_pct": dg.MetadataValue.float(threshold * 100),
            },
        )
    except Exception as e:
        _write_check_result(
            store, check_name, asset_name, False, context, {"error": str(e)}
        )
        return dg.AssetCheckResult(
            passed=False,
            metadata={"error": dg.MetadataValue.text(str(e))},
        )


@dg.asset_check(asset="silver_auth_events")
def silver_auth_events_no_duplicates(
    context: dg.AssetCheckExecutionContext,
    spark: SparkConnectResource,
    streaming_config: StreamingJobConfig,
) -> dg.AssetCheckResult:
    """Check that there are no duplicate event_ids."""
    store = _get_dq_store()
    asset_name = "silver_auth_events"
    check_name = "no_duplicates"

    session = spark.get_session()
    catalog = streaming_config.catalog
    namespace = streaming_config.namespace
    table_name = f"{catalog}.{namespace}.silver_auth_events"

    try:
        df = session.table(table_name)
        total_rows = df.count()

        if total_rows == 0:
            metadata: dict[str, Any] = {
                "total_rows": 0,
                "distinct_event_ids": 0,
                "duplicates": 0,
            }
            _write_check_result(store, check_name, asset_name, True, context, metadata)
            return dg.AssetCheckResult(
                passed=True,
                metadata={"total_rows": dg.MetadataValue.int(0)},
            )

        distinct_event_ids = df.select("event_id").distinct().count()
        duplicates = total_rows - distinct_event_ids
        passed = duplicates == 0

        metadata = {
            "total_rows": total_rows,
            "distinct_event_ids": distinct_event_ids,
            "duplicates": duplicates,
        }
        _write_check_result(store, check_name, asset_name, passed, context, metadata)

        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "total_rows": dg.MetadataValue.int(total_rows),
                "distinct_event_ids": dg.MetadataValue.int(distinct_event_ids),
                "duplicates": dg.MetadataValue.int(duplicates),
            },
        )
    except Exception as e:
        _write_check_result(
            store, check_name, asset_name, False, context, {"error": str(e)}
        )
        return dg.AssetCheckResult(
            passed=False,
            metadata={"error": dg.MetadataValue.text(str(e))},
        )


# =============================================================================
# silver_user_sessions Checks
# =============================================================================


@dg.asset_check(asset="silver_user_sessions")
def silver_user_sessions_not_empty(
    context: dg.AssetCheckExecutionContext,
    spark: SparkConnectResource,
    streaming_config: StreamingJobConfig,
) -> dg.AssetCheckResult:
    """Check that silver_user_sessions is not empty."""
    store = _get_dq_store()
    asset_name = "silver_user_sessions"
    check_name = "not_empty"

    session = spark.get_session()
    catalog = streaming_config.catalog
    namespace = streaming_config.namespace
    table_name = f"{catalog}.{namespace}.silver_user_sessions"

    try:
        df = session.table(table_name)
        session_count = df.count()
        passed = session_count > 0

        metadata: dict[str, Any] = {"session_count": session_count}
        _write_check_result(store, check_name, asset_name, passed, context, metadata)

        return dg.AssetCheckResult(
            passed=passed,
            metadata={"session_count": dg.MetadataValue.int(session_count)},
        )
    except Exception as e:
        _write_check_result(
            store, check_name, asset_name, False, context, {"error": str(e)}
        )
        return dg.AssetCheckResult(
            passed=False,
            metadata={"error": dg.MetadataValue.text(str(e))},
        )


@dg.asset_check(asset="silver_user_sessions")
def silver_user_sessions_valid_duration(
    context: dg.AssetCheckExecutionContext,
    spark: SparkConnectResource,
    streaming_config: StreamingJobConfig,
) -> dg.AssetCheckResult:
    """Check that no session_duration_seconds is negative."""
    store = _get_dq_store()
    asset_name = "silver_user_sessions"
    check_name = "valid_duration"

    session = spark.get_session()
    catalog = streaming_config.catalog
    namespace = streaming_config.namespace
    table_name = f"{catalog}.{namespace}.silver_user_sessions"

    try:
        df = session.table(table_name)
        total_sessions = df.count()

        if total_sessions == 0:
            metadata: dict[str, Any] = {"total_sessions": 0, "negative_duration_count": 0}
            _write_check_result(store, check_name, asset_name, True, context, metadata)
            return dg.AssetCheckResult(
                passed=True,
                metadata={"total_sessions": dg.MetadataValue.int(0)},
            )

        # Check for negative durations
        negative_duration_count = df.filter(col("session_duration_seconds") < 0).count()
        passed = negative_duration_count == 0

        metadata = {
            "total_sessions": total_sessions,
            "negative_duration_count": negative_duration_count,
        }
        _write_check_result(store, check_name, asset_name, passed, context, metadata)

        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "total_sessions": dg.MetadataValue.int(total_sessions),
                "negative_duration_count": dg.MetadataValue.int(negative_duration_count),
            },
        )
    except Exception as e:
        _write_check_result(
            store, check_name, asset_name, False, context, {"error": str(e)}
        )
        return dg.AssetCheckResult(
            passed=False,
            metadata={"error": dg.MetadataValue.text(str(e))},
        )
