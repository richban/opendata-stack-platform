from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

spark: SparkSession


@dp.temporary_view(comment="Raw NYC Yellow Taxi trip data ingested from MinIO datalake")
def raw_yellow_taxi() -> DataFrame:
    return spark.read.parquet(
        "s3a://datalake/raw/yellow/yellow_tripdata_2024-12-01.parquet"
    )  # noqa: F821
