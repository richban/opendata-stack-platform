import os


def get_environment() -> str:
    if os.getenv("ENVIRONMENT", "") == "dev":
        return "dev"
    if os.getenv("ENVIRONMENT", "") == "prod":
        return "prod"
    return "dev"


def get_dbt_target() -> str:
    """Get the appropriate dbt target based on the current environment.

    Returns:
        str: The dbt target name to use. Will be one of:
            - 'dev' for development environment
            - 'branch_deployment' for branch deployments
            - 'prod' for production environment
            - Value of DBT_TARGET env var (defaults to 'personal') for local development
    """
    env = get_environment()
    if env == "dev":
        return "dev"
    if env == "prod":
        return "prod"
    return os.getenv("DBT_TARGET", "personal")


def get_compute_kind() -> str:
    env = get_environment()
    if env == "dev":
        return "DuckDB"
    if env == "prod":
        return "Snowflake"
    return "DuckDB"


def get_sqlmesh_gateway() -> str:
    env = get_environment()
    if env == "dev":
        return "duckdb"
    if env == "prod":
        return "snowflake"
    return "duckdb"
