"""Data Quality result store using SQLite.

This module provides a Dagster ConfigurableResource for storing asset check results
in a local SQLite database with an append-only table design.
"""

import json
import logging
import sqlite3

from pathlib import Path
from typing import Any

import dagster as dg
from pydantic import PrivateAttr, field_validator


class _DQResultStoreClient:
    """Stateful client for database operations.

    This separate class holds runtime state since DQResultStore is frozen.
    """

    def __init__(self, db_path: Path, log_path: Path) -> None:
        self._db_path = db_path
        self._log_path = log_path
        self._logger: logging.Logger | None = None

        # Ensure directories exist
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._log_path.parent.mkdir(parents=True, exist_ok=True)

        # Initialize
        self._setup_logging()
        self._init_db()

    def _setup_logging(self) -> None:
        """Configure file logging for DQ check results."""
        self._logger = logging.getLogger(f"dq_checks_{id(self)}")
        self._logger.setLevel(logging.INFO)

        # Remove existing handlers to avoid duplicates
        self._logger.handlers = []

        # Add file handler
        file_handler = logging.FileHandler(self._log_path)
        file_handler.setLevel(logging.INFO)

        # Simple format: timestamp - level - message
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(formatter)

        self._logger.addHandler(file_handler)

    def _init_db(self) -> None:
        """Initialize SQLite database with dq_results table."""
        with sqlite3.connect(self._db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dq_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    check_name TEXT NOT NULL,
                    asset_name TEXT NOT NULL,
                    passed BOOLEAN NOT NULL,
                    run_id TEXT,
                    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    metadata_json TEXT
                )
            """)
            conn.commit()

    def write_result(
        self,
        check_name: str,
        asset_name: str,
        passed: bool,
        run_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Write a check result to the SQLite store and log file."""
        # Write to SQLite
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(
                """
                INSERT INTO dq_results
                (check_name, asset_name, passed, run_id, metadata_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    check_name,
                    asset_name,
                    passed,
                    run_id,
                    json.dumps(metadata) if metadata else None,
                ),
            )
            conn.commit()

        # Log to file
        if self._logger:
            status = "PASSED" if passed else "FAILED"
            metadata_str = json.dumps(metadata) if metadata else "{}"
            log_message = (
                f"Check '{check_name}' on asset '{asset_name}' {status}. "
                f"Run ID: {run_id or 'N/A'}. Metadata: {metadata_str}"
            )

            if passed:
                self._logger.info(log_message)
            else:
                self._logger.warning(log_message)

    def get_results(
        self,
        asset_name: str | None = None,
        check_name: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Retrieve check results from the database."""
        query = "SELECT * FROM dq_results WHERE 1=1"
        params: list[Any] = []

        if asset_name:
            query += " AND asset_name = ?"
            params.append(asset_name)

        if check_name:
            query += " AND check_name = ?"
            params.append(check_name)

        query += " ORDER BY checked_at DESC LIMIT ?"
        params.append(limit)

        with sqlite3.connect(self._db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)
            rows = cursor.fetchall()

            results = []
            for row in rows:
                result = dict(row)
                if result.get("metadata_json"):
                    result["metadata"] = json.loads(result["metadata_json"])
                    del result["metadata_json"]
                else:
                    result["metadata"] = None
                    del result["metadata_json"]
                results.append(result)

            return results


class DQResultStore(dg.ConfigurableResource):
    """Store for data quality check results.

    Stores check results in a local SQLite database with an append-only table.
    Also logs check results to a file using Python's logging module.

    Configuration fields:
        db_path: Path to SQLite database file (default: dq_results/dq_checks.db)
        log_path: Path to log file (default: dq_results/dq_checks.log)

    Example:
        >>> store = DQResultStore(db_path="custom/path/dq.db")
        >>> store.write_result(
        ...     check_name="not_null",
        ...     asset_name="my_table",
        ...     passed=True,
        ...     run_id="12345",
        ... )
    """

    db_path: str = "dq_results/dq_checks.db"
    log_path: str = "dq_results/dq_checks.log"

    # Private attr for the stateful client
    _client: _DQResultStoreClient | None = PrivateAttr(default=None)

    @field_validator("db_path", "log_path", mode="before")
    @classmethod
    def _convert_path_to_str(cls, v: Any) -> str:
        """Convert Path objects to strings."""
        if isinstance(v, Path):
            return str(v)
        return v

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        """Initialize the resource for execution."""
        self._get_client()

    def _get_client(self) -> _DQResultStoreClient:
        """Get or create the stateful client."""
        if self._client is None:
            # Ensure paths are strings, convert from Path if necessary
            db_path_str = str(self.db_path)
            log_path_str = str(self.log_path)
            self._client = _DQResultStoreClient(
                db_path=Path(db_path_str),
                log_path=Path(log_path_str),
            )
        return self._client

    def write_result(
        self,
        check_name: str,
        asset_name: str,
        passed: bool,
        run_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Write a check result to the SQLite store and log file.

        Args:
            check_name: Name of the check (e.g., "not_empty")
            asset_name: Name of the asset being checked
            passed: Whether the check passed
            run_id: Optional Dagster run ID
            metadata: Optional metadata dictionary (stored as JSON)
        """
        self._get_client().write_result(
            check_name=check_name,
            asset_name=asset_name,
            passed=passed,
            run_id=run_id,
            metadata=metadata,
        )

    def get_results(
        self,
        asset_name: str | None = None,
        check_name: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Retrieve check results from the database.

        Args:
            asset_name: Filter by asset name (optional)
            check_name: Filter by check name (optional)
            limit: Maximum number of results to return

        Returns:
            List of result dictionaries
        """
        return self._get_client().get_results(
            asset_name=asset_name,
            check_name=check_name,
            limit=limit,
        )
