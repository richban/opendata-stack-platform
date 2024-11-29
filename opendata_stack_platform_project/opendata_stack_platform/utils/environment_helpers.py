import os

def get_environment() -> str:
    if os.getenv("DAGSTER_ORGANIZATION", "") == "dev":
        return "DEV"
    if os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT", "") == "1":
        return "BRANCH"
    if os.getenv("DAGSTER_CLOUD_DEPLOYMENT_NAME", "") == "prod":
        return "PROD"
    return "LOCAL"


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
    if env == "DEV":
        return "dev"
    if env == "BRANCH":
        return "branch_deployment"
    if env == "PROD":
        return "prod"
    return os.getenv("DBT_TARGET", "personal")