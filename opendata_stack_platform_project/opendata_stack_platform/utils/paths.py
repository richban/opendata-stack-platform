"""Utility functions for handling paths in the project."""

import os
from pathlib import Path


def get_project_root() -> Path:
    """Get the absolute path to the project root directory.
    
    Returns:
        Path: Absolute path to the project root directory
    """
    # This file is in opendata_stack_platform/utils/paths.py
    # Go up three levels to get to the project root
    return Path(__file__).parent.parent.parent.parent


def get_duckdb_path() -> Path:
    """Get the absolute path to the DuckDB database file.
    
    Returns:
        Path: Absolute path to the DuckDB database file
    """
    # First try environment variable
    if db_path := os.getenv("DUCKDB_DATABASE"):
        return Path(db_path)
    
    # Default to data/nyc_database.duckdb in project root
    return get_project_root() / "data" / "nyc_database.duckdb"
