from pathlib import Path

from dagster_dbt import DbtCliResource, DbtProject
from opendata_stack_platform.utils.environment_helpers import get_dbt_target

opendata_stack_platform_dbt_project = DbtProject(
    project_dir=Path(__file__)
    .joinpath("..", "..", "..", "..", "opendata_stack_platform_dbt")
    .resolve(),
    target=get_dbt_target(),
)

# If `dagster dev` is used, the dbt project will be prepared to create the manifest at run time.
# Otherwise, we expect a manifest to be present in the project's target directory.
opendata_stack_platform_dbt_project.prepare_if_dev()

dbt_resource = DbtCliResource(project_dir=opendata_stack_platform_dbt_project)
