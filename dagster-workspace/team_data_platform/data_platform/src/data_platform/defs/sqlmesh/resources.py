from pathlib import Path

from dagster_sqlmesh import SQLMeshContextConfig, SQLMeshResource
from data_platform.utils.environment_helpers import get_sqlmesh_gateway


# Get the workspace root directory (8 levels up from the current file)
REPO_ROOT = Path(__file__).parent.parent.parent.parent.parent.parent.parent.parent

# Define the dbt project path relative to repository root
SQLMESH_PROJECT_PATH = REPO_ROOT / "opendata_stack_platform_sqlmesh"


sqlmesh_config = SQLMeshContextConfig(
    path=str(SQLMESH_PROJECT_PATH),
    gateway=get_sqlmesh_gateway(),
    translator_class_name="data_platform.defs.sqlmesh.assets.CustomSQLMeshTranslator",
)
sqlmesh_resource = SQLMeshResource(config=sqlmesh_config)
