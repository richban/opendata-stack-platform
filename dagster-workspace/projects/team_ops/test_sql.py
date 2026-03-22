import os
from dotenv import load_dotenv

load_dotenv(".env.polaris")

from team_ops.defs.resources import create_spark_session

spark = create_spark_session()
print("Connected. Running SQL...")
try:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.streamify").collect()
    print("Done")
except Exception as e:
    print("Exception!")
    import traceback

    traceback.print_exc()
print("Exiting normally.")
