"""Main Dagster definitions for team_ops.

This module loads definitions from the defs folder and exposes them
to the Dagster workspace.
"""

from team_ops.defs.definitions import defs

__all__ = ["defs"]
