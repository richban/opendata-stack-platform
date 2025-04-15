from pathlib import Path

from dagster_dbt import DbtCliResource, DbtProject

from data_platform.utils.environment_helpers import get_dbt_target

# Get the workspace root directory (8 levels up from the current file)
REPO_ROOT = Path(__file__).parent.parent.parent.parent.parent.parent.parent.parent

# Define the dbt project path relative to repository root
DBT_PROJECT_PATH = REPO_ROOT / "opendata_stack_platform_dbt"


opendata_stack_platform_dbt_project = DbtProject(
    project_dir=DBT_PROJECT_PATH,
    target=get_dbt_target(),
)

opendata_stack_platform_dbt_project.prepare_if_dev()

dbt_resource = DbtCliResource(project_dir=opendata_stack_platform_dbt_project)
