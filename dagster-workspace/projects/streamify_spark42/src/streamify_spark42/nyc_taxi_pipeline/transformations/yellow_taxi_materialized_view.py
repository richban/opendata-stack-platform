from pyspark.sql import SparkSession
from pyspark import pipelines as dp
from pyspark.sql import DataFrame

spark: SparkSession


@dp.materialized_view(
    comment="Raw NYC Yellow Taxi trip data ingested from MinIO datalake"
)
def raw_yellow_taxi() -> DataFrame:
    return spark.read.parquet("s3a://datalake/raw/yellow/yellow_tripdata_2024-12-01.parquet")

