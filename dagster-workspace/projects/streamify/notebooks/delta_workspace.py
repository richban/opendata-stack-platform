import marimo

__generated_with = "0.21.1"
app = marimo.App(width="full")


@app.cell
def _():
    import datetime as dt
    import random
    import sys

    from pathlib import Path

    import pyspark.sql.functions as F
    import pyspark.sql.types as T

    from delta.tables import DeltaTable

    sys.path.insert(0, str(Path(__file__).parent))

    from config import create_delta_spark_session, get_s3_store

    return (
        DeltaTable,
        F,
        T,
        create_delta_spark_session,
        dt,
        get_s3_store,
        random,
    )


@app.cell
def _(create_delta_spark_session, get_s3_store):
    store = get_s3_store()
    spark, conn = create_delta_spark_session("s3a://lakehouse/delta")

    # Ensure the Spark catalog is set up for S3 path-based tables.
    spark.sql("CREATE DATABASE IF NOT EXISTS delta LOCATION 's3a://lakehouse/delta'")
    return (spark,)


@app.cell
def _(T):
    schema = T.StructType(
        [
            T.StructField(
                "estimation_mode", T(), False
            ),  # A=annual or M=monthly
            T.StructField("account_id", T.StringType(), False),  # natural key
            T.StructField("year", T.IntegerType(), False),  # year in question
            T.StructField("month", T.DateType(), True),  # null when estimation_mode='A'
            T.StructField("currency", T.StringType(), False),
            T.StructField("estimate", T.DoubleType(), False),
            T.StructField("inserted_at", T.TimestampType(), False),
            T.StructField("batch_id", T.StringType(), False),
        ]
    )
    return (schema,)


@app.cell
def _(dt, random, schema, spark):
    def make_batch(rows):
        batch_id = str(random.randint(0, 9999)).zfill(4)
        now = dt.datetime.now()
        data = [(*row, now, batch_id) for row in rows]
        return spark.createDataFrame(data, schema=schema)

    return (make_batch,)


@app.cell
def _(make_batch):
    batch_1 = make_batch(
        [
            ("A", str(1).zfill(4), 2026, None, "USD", 10000.0),
            ("A", str(2).zfill(4), 2026, None, "USD", 12000.0),
        ],
    )
    return (batch_1,)


@app.cell
def _():
    bronze_path = "s3a://lakehouse/delta/bronze_estimates"

    def append_bronze(df):
        df.write.format("delta").mode("append").save(bronze_path)
        return bronze_path

    return (append_bronze,)


@app.cell
def _(append_bronze, batch_1):
    append_bronze(batch_1)


@app.cell
def _(DeltaTable, F, schema, spark):
    silver_path = "s3a://lakehouse/delta/silver_estimates"

    def init_silver():
        DeltaTable.createIfNotExists(spark) \
            .location(silver_path) \
            .addColumns(schema) \
            .property("delta.enableChangeDataFeed", "true") \
            .execute()
        return silver_path

    def merge_to_silver(batch_df):
        # Step 1: identify business contexts touched by this batch
        contexts = batch_df.select("account_id", "year", "currency").distinct()

        # Step 2: build tombstones for any existing silver rows in those contexts.
        # We read the current silver state and tag every matched row for deletion.
        silver_existing = (
            spark.read.format("delta")
            .load(silver_path)
            .join(contexts, ["account_id", "year", "currency"])
            .withColumn("_delete_flag", F.lit(True))
        )

        # Step 3: prepare incoming rows (no tombstone flag)
        incoming = batch_df.withColumn("_delete_flag", F.lit(False))

        # Step 4: union tombstones + incoming, then atomic MERGE.
        if silver_existing is not None:
            merge_source = silver_existing.unionByName(incoming, allowMissingColumns=True)
        else:
            merge_source = incoming

        delta_table = DeltaTable.forPath(spark, silver_path)

        (
            delta_table.alias("t")
            .merge(
                merge_source.alias("s"),
                """t.account_id = s.account_id
                   AND t.year = s.year
                   AND t.currency = s.currency
                   AND t.estimation_mode = s.estimation_mode
                   AND (t.month = s.month OR (t.month IS NULL AND s.month IS NULL))""",
            )
            .whenMatchedDelete(condition="s._delete_flag = true")
            .whenMatchedUpdateAll(condition="s._delete_flag = false")
            .whenNotMatchedInsertAll(condition="s._delete_flag = false")
            .execute()
        )
        return silver_path

    return init_silver, merge_to_silver, silver_path


@app.cell
def _(batch_1, init_silver, merge_to_silver):
    init_silver()
    merge_to_silver(batch_1)


@app.cell
def _(dt, make_batch):
    # Batch 2: change account 0001 from annual to monthly (grain switch A -> M)
    # Keep account 0002 unchanged (but we don't include it in this batch)
    # Add a new account 0004 as annual.
    batch_2 = make_batch(
        [
            ("M", str(1).zfill(4), 2026, dt.date(2026, 1, 1), "USD", 3000.0),
            ("M", str(1).zfill(4), 2026, dt.date(2026, 2, 1), "USD", 3200.0),
            ("M", str(1).zfill(4), 2026, dt.date(2026, 3, 1), "USD", 3100.0),
            ("A", str(4).zfill(4), 2026, None, "USD", 15000.0),
        ],
        batch_id="B002",
    )
    return (batch_2,)


@app.cell
def _(append_bronze, batch_2, merge_to_silver):
    append_bronze(batch_2)
    merge_to_silver(batch_2)


@app.cell
def _(silver_path, spark):
    silver_df = (
        spark.read.format("delta").load(silver_path).orderBy("account_id", "month")
    )
    silver_df.show(truncate=False)


@app.cell
def _(silver_path, spark):
    # Demonstrate downstream consumption of the Silver CDF.
    # This shows every row that changed in Silver across all versions.
    cdf_df = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0)
        .load(silver_path)
        .orderBy("_commit_version", "account_id", "month")
    )
    cdf_df.show(truncate=False)


@app.cell
def _(spark):
    # Downstream pattern: extract distinct business contexts that changed in the
    # latest Silver version. A real pipeline would use this key list to pull the
    # full current context from Silver and re-run calculations.
    latest_version = (
        spark.read.format("delta")
        .load("s3a://lakehouse/delta/silver_estimates")
        .write.format("noop")
        .mode("overwrite")
        .save()
    )


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
