import argparse

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


def create_spark_session(app_name: str = "StreamifyHourlyBatch") -> SparkSession:
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
        .config("spark.sql.catalog.local.warehouse", "/data/warehouse")
        .getOrCreate()
    )


def load_hour_data(
    spark: SparkSession,
    topic: str,
    year: int,
    month: int,
    day: int,
    hour: int,
    lake_base_path: str = "/data/lake",
    catalog: str = "local",
    database: str = "streamify",
):
    table_name = f"{topic}_staging"
    full_table_name = f"{catalog}.{database}.{table_name}"

    data_path = f"{lake_base_path}/{topic}/month={month}/day={day}/hour={hour}"

    print(
        f"Loading data from {data_path} into {full_table_name} for {year}-{month:02d}-{day:02d} {hour:02d}:00"
    )

    try:
        df = spark.read.parquet(data_path)

        df = df.withColumn("load_timestamp", lit(datetime.now()))

        record_count = df.count()
        print(f"Found {record_count} records for {topic}")

        if record_count > 0:
            df.writeTo(full_table_name).using("iceberg").append()
            print(f"Successfully loaded {record_count} records into {full_table_name}")
        else:
            print(f"No records to load for {topic}")

        return record_count

    except Exception as e:
        print(f"Error loading data for {topic}: {e}")
        return 0


def run_hourly_batch(
    year: int,
    month: int,
    day: int,
    hour: int,
    lake_base_path: str = "/data/lake",
    catalog: str = "local",
    database: str = "streamify",
):
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")

    topics = ["listen_events", "page_view_events", "auth_events"]

    total_records = 0
    for topic in topics:
        table_name = f"{topic}_staging"
        full_table_name = f"{catalog}.{database}.{table_name}"

        print(f"Ensuring table exists: {full_table_name}")

        count = load_hour_data(
            spark,
            topic,
            year,
            month,
            day,
            hour,
            lake_base_path,
            catalog,
            database,
        )
        total_records += count

    print(
        f"Batch load complete. Total records loaded: {total_records} for {year}-{month:02d}-{day:02d} {hour:02d}:00"
    )

    spark.stop()


def main():
    parser = argparse.ArgumentParser(
        description="Hourly batch load from data lake to Iceberg"
    )
    parser.add_argument("--year", type=int, required=True, help="Year of data to load")
    parser.add_argument("--month", type=int, required=True, help="Month of data to load")
    parser.add_argument("--day", type=int, required=True, help="Day of data to load")
    parser.add_argument("--hour", type=int, required=True, help="Hour of data to load")
    parser.add_argument(
        "--lake-path", default="/data/lake", help="Base path for data lake"
    )
    parser.add_argument("--catalog", default="local", help="Iceberg catalog name")
    parser.add_argument("--database", default="streamify", help="Database name")

    args = parser.parse_args()

    run_hourly_batch(
        year=args.year,
        month=args.month,
        day=args.day,
        hour=args.hour,
        lake_base_path=args.lake_path,
        catalog=args.catalog,
        database=args.database,
    )


if __name__ == "__main__":
    main()
