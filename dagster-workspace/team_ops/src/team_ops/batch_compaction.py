import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


def create_spark_session(
    app_name: str = "TelemetryBatchCompaction",
) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "/data/iceberg-warehouse")
        .getOrCreate()
    )


def run_batch_compaction(
    source_catalog: str = "local",
    source_database: str = "telemetry",
    source_table: str = "raw_events",
    target_catalog: str = "local",
    target_database: str = "telemetry",
    target_table: str = "gold_events",
):
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    source_full_name = f"{source_catalog}.{source_database}.{source_table}"
    target_full_name = f"{target_catalog}.{target_database}.{target_table}"

    print(f"Starting Batch Compaction: {source_full_name} -> {target_full_name}")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_catalog}.{target_database}")

    raw_df = spark.table(source_full_name)

    print(f"Read {raw_df.count()} records from {source_full_name}")

    window_spec = Window.partitionBy("event_id").orderBy(
        col("ingestion_timestamp").desc()
    )

    deduplicated_df = (
        raw_df.withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    deduped_count = deduplicated_df.count()
    print(f"Deduplicated to {deduped_count} unique records")

    deduplicated_df.writeTo(target_full_name).using("iceberg").createOrReplace()

    print(f"Successfully wrote {deduped_count} records to {target_full_name}")

    spark.stop()


def main():
    parser = argparse.ArgumentParser(
        description="Spark Batch Compaction and Deduplication"
    )
    parser.add_argument("--source-catalog", default="local", help="Source catalog name")
    parser.add_argument(
        "--source-database", default="telemetry", help="Source database name"
    )
    parser.add_argument("--source-table", default="raw_events", help="Source table name")
    parser.add_argument("--target-catalog", default="local", help="Target catalog name")
    parser.add_argument(
        "--target-database", default="telemetry", help="Target database name"
    )
    parser.add_argument("--target-table", default="gold_events", help="Target table name")

    args = parser.parse_args()

    run_batch_compaction(
        source_catalog=args.source_catalog,
        source_database=args.source_database,
        source_table=args.source_table,
        target_catalog=args.target_catalog,
        target_database=args.target_database,
        target_table=args.target_table,
    )


if __name__ == "__main__":
    main()
