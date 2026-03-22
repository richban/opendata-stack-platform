"""Tests for Data Quality checks.

This module contains unit tests for the DQ check functions.
Tests focus on verifying DQResultStore functionality and that checks are loadable.
"""

import sqlite3

from pathlib import Path

import dagster as dg

from streamify.defs import dq_checks
from streamify.defs.dq_checks import (
    silver_listen_events_not_empty,
    silver_user_sessions_not_empty,
)
from streamify.defs.dq_store import DQResultStore

EXPECTED_RESULT_COUNT = 3
EXPECTED_ROW_COUNT = 1000


class TestDQResultStore:
    """Tests for the DQResultStore class."""

    def test_store_creates_database_and_table(self, tmp_path: Path) -> None:
        """Test that DQResultStore creates database and table on init."""
        db_path = tmp_path / "test_dq.db"
        log_path = tmp_path / "test_dq.log"

        _store = DQResultStore(db_path=db_path, log_path=log_path)
        del _store

        assert db_path.exists()

        # Verify table was created
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='dq_results'"
            )
            assert cursor.fetchone() is not None

    def test_write_result_stores_data(self, tmp_path: Path) -> None:
        """Test that write_result stores data in SQLite and logs to file."""
        db_path = tmp_path / "test_dq.db"
        log_path = tmp_path / "test_dq.log"

        store = DQResultStore(db_path=db_path, log_path=log_path)

        store.write_result(
            check_name="not_empty",
            asset_name="silver_listen_events",
            passed=True,
            run_id="test-run-123",
            metadata={"row_count": 100},
        )

        # Verify data in SQLite
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("SELECT * FROM dq_results")
            row = cursor.fetchone()
            assert row is not None
            assert row[1] == "not_empty"  # check_name
            assert row[2] == "silver_listen_events"  # asset_name
            assert row[3] == 1  # passed (SQLite stores bool as 0/1)
            assert row[4] == "test-run-123"  # run_id

        # Verify log file was written
        assert log_path.exists()
        log_content = log_path.read_text()
        assert "not_empty" in log_content
        assert "silver_listen_events" in log_content
        assert "PASSED" in log_content

    def test_write_result_logs_failed_check_as_warning(self, tmp_path: Path) -> None:
        """Test that failed checks are logged as warnings."""
        db_path = tmp_path / "test_dq.db"
        log_path = tmp_path / "test_dq.log"

        store = DQResultStore(db_path=db_path, log_path=log_path)

        store.write_result(
            check_name="not_empty",
            asset_name="silver_listen_events",
            passed=False,
            metadata={"row_count": 0},
        )

        log_content = log_path.read_text()
        assert "FAILED" in log_content

    def test_get_results_returns_data(self, tmp_path: Path) -> None:
        """Test that get_results retrieves data correctly."""
        db_path = tmp_path / "test_dq.db"
        log_path = tmp_path / "test_dq.log"

        store = DQResultStore(db_path=db_path, log_path=log_path)

        # Write multiple results
        store.write_result(
            check_name="not_empty",
            asset_name="silver_listen_events",
            passed=True,
            metadata={"row_count": 100},
        )
        store.write_result(
            check_name="no_nulls",
            asset_name="silver_listen_events",
            passed=False,
            metadata={"null_count": 5},
        )

        expected_two_results = 2
        expected_one_result = 1

        # Get all results
        results = store.get_results()
        assert len(results) == expected_two_results

        # Filter by asset
        results = store.get_results(asset_name="silver_listen_events")
        assert len(results) == expected_two_results

        # Filter by check
        results = store.get_results(check_name="not_empty")
        assert len(results) == expected_one_result
        assert results[0]["check_name"] == "not_empty"


class TestDQChecksDefinitions:
    """Tests that DQ checks are properly registered in definitions."""

    def test_dq_checks_module_loads(self) -> None:
        """Test that the dq_checks module can be imported."""
        # Verify check functions exist
        assert hasattr(dq_checks, "silver_listen_events_not_empty")
        assert hasattr(dq_checks, "silver_listen_events_no_nulls")
        assert hasattr(dq_checks, "silver_listen_events_no_duplicates")
        assert hasattr(dq_checks, "silver_page_view_events_not_empty")
        assert hasattr(dq_checks, "silver_page_view_events_no_nulls")
        assert hasattr(dq_checks, "silver_page_view_events_no_duplicates")
        assert hasattr(dq_checks, "silver_auth_events_not_empty")
        assert hasattr(dq_checks, "silver_auth_events_no_nulls")
        assert hasattr(dq_checks, "silver_auth_events_no_duplicates")
        assert hasattr(dq_checks, "silver_user_sessions_not_empty")
        assert hasattr(dq_checks, "silver_user_sessions_valid_duration")

    def test_dq_checks_are_asset_checks(self) -> None:
        """Test that DQ check functions are properly decorated as asset checks."""
        # Verify they are AssetChecksDefinition objects
        assert isinstance(silver_listen_events_not_empty, dg.AssetChecksDefinition)
        assert isinstance(silver_user_sessions_not_empty, dg.AssetChecksDefinition)

    def test_dq_store_integration(self, tmp_path: Path) -> None:
        """Test DQResultStore integration with check metadata writing."""
        db_path = tmp_path / "integration.db"
        log_path = tmp_path / "integration.log"

        store = DQResultStore(db_path=db_path, log_path=log_path)

        # Simulate check results being written
        check_results = [
            {
                "check_name": "not_empty",
                "asset_name": "silver_listen_events",
                "passed": True,
                "metadata": {"row_count": EXPECTED_ROW_COUNT},
            },
            {
                "check_name": "no_nulls",
                "asset_name": "silver_listen_events",
                "passed": True,
                "metadata": {"total_rows": EXPECTED_ROW_COUNT, "null_count": 0},
            },
            {
                "check_name": "no_duplicates",
                "asset_name": "silver_listen_events",
                "passed": True,
                "metadata": {"total_rows": EXPECTED_ROW_COUNT, "duplicates": 0},
            },
        ]

        for result in check_results:
            store.write_result(
                check_name=result["check_name"],
                asset_name=result["asset_name"],
                passed=result["passed"],
                run_id="integration-test-123",
                metadata=result["metadata"],
            )

        # Verify all results were stored
        results = store.get_results(asset_name="silver_listen_events")
        assert len(results) == EXPECTED_RESULT_COUNT

        # Verify specific check
        not_empty_results = store.get_results(
            asset_name="silver_listen_events", check_name="not_empty"
        )
        expected_single_result = 1
        assert len(not_empty_results) == expected_single_result
        # SQLite returns 1 for True, handle both cases
        assert not_empty_results[0]["passed"] in (True, 1)
        assert not_empty_results[0]["metadata"]["row_count"] == EXPECTED_ROW_COUNT
