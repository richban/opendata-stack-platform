from dagster import EnvVar
from dagster_duckdb import DuckDBResource

duckdb_resource = DuckDBResource(
    database=EnvVar("DUCKDB_DATABASE"),
    connection_config={
        "s3_endpoint": EnvVar("AWS_ENDPOINT_URL").get_value().replace("http://", ""),
        "s3_access_key_id": EnvVar("AWS_ACCESS_KEY_ID").get_value(),
        "s3_secret_access_key": EnvVar("AWS_SECRET_ACCESS_KEY").get_value(),
        "s3_region": EnvVar("AWS_REGION").get_value(),
        "s3_use_ssl": False,
        "s3_url_style": "path",
    },
)
