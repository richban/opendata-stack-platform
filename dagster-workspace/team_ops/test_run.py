import os
from dotenv import load_dotenv

load_dotenv(".env.polaris")

from dagster import build_op_context
from team_ops.defs.assets import bronze_streaming_job
from team_ops.defs.definitions import defs

from team_ops.defs.resources import create_spark_session, create_streaming_config

spark = create_spark_session()
streaming_config = create_streaming_config()
context = build_op_context(
    resources={"spark": spark, "streaming_config": streaming_config}
)

try:
    for res in bronze_streaming_job(context):
        print(res)
except Exception as e:
    import traceback

    traceback.print_exc()
