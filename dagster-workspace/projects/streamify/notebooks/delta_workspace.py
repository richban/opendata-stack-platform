import marimo

__generated_with = "0.21.1"
app = marimo.App(width="full")


@app.cell
def _():
    import datetime as dt
    import random
    import sys
    import uuid

    from pathlib import Path

    import marimo as mo
    import pyspark.sql.functions as F
    import pyspark.sql.types as T

    from delta.tables import DeltaTable
    from pyspark.sql import DataFrame

    sys.path.insert(0, str(Path(__file__).parent))

    from config import create_delta_spark_session, get_s3_store

    return (
        DataFrame,
        DeltaTable,
        F,
        T,
        create_delta_spark_session,
        dt,
        get_s3_store,
        mo,
        random,
        uuid,
    )


@app.cell
def _(create_delta_spark_session, get_s3_store):
    store = get_s3_store()
    spark, conn = create_delta_spark_session("s3a://lakehouse/delta")

    # Ensure the Spark catalog is set up for S3 path-based tables.
    # spark.sql("CREATE DATABASE IF NOT EXISTS delta LOCATION 's3a://lakehouse/delta'")
    return conn, spark


@app.cell
def _(conn):
    conn.list_databases()
    return


@app.cell
def _(T):
    schema = T.StructType(
        [
            T.StructField("estimation_mode", T.StringType(), False),
            T.StructField("account_id", T.StringType(), False),  # natural key
            T.StructField("year", T.IntegerType(), False),  # year in question
            T.StructField("month", T.DateType(), True),  # null when estimation_mode='A'
            T.StructField("currency", T.StringType(), False),
            T.StructField("estimate", T.DoubleType(), False),
        ]
    )

    bronze_schema = T.StructType(
        [
            *schema.fields,
            T.StructField("inserted_at", T.TimestampType(), True),
            T.StructField("batch_id", T.StringType(), False),
            T.StructField("event_id", T.StringType(), False),
        ]
    )
    silver_schema = T.StructType(
        [
            *schema.fields,
            T.StructField("updated_at", T.TimestampType(), False),
            T.StructField("event_id", T.StringType(), False),
        ]
    )
    gold_schema = T.StructType(
        [
            *schema.fields,
            T.StructField("event_id", T.StringType(), False),
            T.StructField("last_calculated_at", T.TimestampType(), False),
            T.StructField("computed_estimate", T.DoubleType(), False),
            T.StructField("model_version", T.StringType(), False),
        ]
    )
    return bronze_schema, gold_schema, silver_schema


@app.cell
def _(bronze_schema, random, spark, uuid):
    def make_batch(rows):
        batch_id = str(random.randint(0, 9999)).zfill(4)
        data = [(*row, None, batch_id, str(uuid.uuid4())) for row in rows]
        return spark.createDataFrame(data, schema=bronze_schema)

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
def _(DeltaTable, F, bronze_schema, spark):
    bronze_path = "s3a://lakehouse/delta/bronze_estimates"

    def init_bronze():
        DeltaTable.createIfNotExists(spark).location(bronze_path).tableName(
            "bronze_estimates"
        ).addColumns(bronze_schema).execute()
        return bronze_path

    def append_bronze(df):
        df.withColumn("inserted_at", F.current_timestamp()).write.format("delta").mode(
            "append"
        ).save(bronze_path)
        return bronze_path

    return append_bronze, init_bronze


@app.cell
def _(append_bronze, batch_1, init_bronze):
    init_bronze()
    append_bronze(batch_1)
    return


@app.cell
def _(batch_1):
    batch_1
    return


@app.cell
def _(bronze_estimates, conn, mo):
    _df = mo.sql(
        """
        select * from bronze_estimates
        """,
        engine=conn,
    )
    return


@app.cell
def _(DataFrame, DeltaTable, F, silver_schema, spark):

    silver_path = "s3a://lakehouse/delta/silver_estimates"

    def init_silver():
        DeltaTable.createIfNotExists(spark).tableName("silver_estimates").location(
            silver_path
        ).addColumns(silver_schema).property(
            "delta.enableChangeDataFeed", "true"
        ).execute()
        return silver_path

    def detect_mode_switch(source_df: DataFrame, target_df: DataFrame) -> DataFrame:
        """
        Find existing target (Silver) rows that must be deleted because the
        estimation mode for their business context changed in the incoming batch.

        """
        # Deduplicated source keys: one row per exact natural key in the batch
        source_keys = (
            source_df.select("account_id", "year", "currency", "estimation_mode")
            .distinct()
            .withColumnRenamed("estimation_mode", "src_mode")
        )
        # Keep target rows that share the context BUT whose mode differs from source
        return (
            target_df.join(source_keys, ["account_id", "year", "currency"], "inner")
            .filter(F.col("estimation_mode") != F.col("src_mode"))
            .drop("src_mode")
        )

    def merge_to_silver(batch_df):
        """
        Atomically merge one batch into Silver using tombstones for mode switches.

        Behaviour:
          * Same mode, same row     -> UPDATE (idempotent re-run)
          * Same mode, new row      -> INSERT
          * Mode switch (A <-> M)   -> DELETE old grain, INSERT new grain
        """
        # Read existing Silver rows for the business contexts touched by this batch.
        contexts = batch_df.select("account_id", "year", "currency").distinct()

        silver_existing = (
            spark.read.format("delta")
            .load(silver_path)
            .join(contexts, ["account_id", "year", "currency"])
        )

        switch_rows = detect_mode_switch(batch_df, silver_existing)
        tombstones = switch_rows.withColumn("_delete_flag", F.lit(True))

        # Tag incoming rows as normal (not tombstones).
        incoming = batch_df.withColumn("_delete_flag", F.lit(False))

        merge_source = tombstones.unionByName(incoming, allowMissingColumns=True)

        # Single atomic MERGE transaction.
        # Explicit column mappings ensure the temporary _delete_flag never
        # leaks into the Silver table schema.
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
            .whenMatchedUpdate(
                condition="s._delete_flag = false",
                set={
                    "estimate": "s.estimate",
                    "updated_at": "current_timestamp()",
                    "event_id": "s.event_id",
                },
            )
            .whenNotMatchedInsert(
                condition="s._delete_flag = false",
                values={
                    "estimation_mode": "s.estimation_mode",
                    "account_id": "s.account_id",
                    "year": "s.year",
                    "month": "s.month",
                    "currency": "s.currency",
                    "estimate": "s.estimate",
                    "updated_at": "current_timestamp()",
                    "event_id": "s.event_id",
                },
            )
            .execute()
        )
        return delta_table

    return init_silver, merge_to_silver, silver_path


@app.cell
def _(DeltaTable, F, gold_schema, silver_path, spark):
    gold_path = "s3a://lakehouse/delta/gold_estimates"

    def init_gold():
        DeltaTable.createIfNotExists(spark).tableName("gold_estimates").location(
            gold_path
        ).addColumns(gold_schema).execute()

    def propagate_to_gold(batch_df, batch_id):
        """ForeachBatch callback: hydrate changed contexts and MERGE into Gold."""
        # Extract changed business context keys from CDF batch
        changed_keys = batch_df.select("account_id", "year", "currency").distinct()

        # Hydrate: pull full current context from Silver
        full_context = (
            spark.read.format("delta")
            .load(silver_path)
            .join(changed_keys, ["account_id", "year", "currency"], "left_semi")
        )

        # Black box: placeholder calculation (constant multiplier)
        computed = (
            full_context.withColumn("computed_estimate", F.col("estimate") * F.lit(1.5))
            .withColumn("model_version", F.lit("v0.1.0-placeholder"))
            .withColumn("last_calculated_at", F.current_timestamp())
        )

        # MERGE into Gold on natural key, but only if Silver event changed
        delta_table = DeltaTable.forPath(spark, gold_path)
        (
            delta_table.alias("t")
            .merge(
                computed.alias("s"),
                """t.account_id = s.account_id
                   AND t.year = s.year
                   AND t.currency = s.currency
                   AND t.estimation_mode = s.estimation_mode
                   AND (t.month = s.month OR (t.month IS NULL AND s.month IS NULL))""",
            )
            .whenMatchedUpdate(
                condition="t.event_id <> s.event_id",
                set={
                    "estimate": "s.estimate",
                    "event_id": "s.event_id",
                    "last_calculated_at": "s.last_calculated_at",
                    "computed_estimate": "s.computed_estimate",
                    "model_version": "s.model_version",
                },
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    def start_gold_pipeline():
        """Run Structured Streaming in batch mode (availableNow=True).

        Spark manages the checkpoint internally so only new CDF changes
        are processed on each invocation.
        """
        checkpoint_dir = "s3a://lakehouse/delta/checkpoints/gold"

        query = (
            spark.readStream.format("delta")
            .option("readChangeFeed", "true")
            .load(silver_path)
            .writeStream.foreachBatch(propagate_to_gold)
            .option("checkpointLocation", checkpoint_dir)
            .trigger(availableNow=True)
            .start()
        )

        query.awaitTermination()
        return query

    return gold_path, init_gold, start_gold_pipeline


@app.cell
def _(batch_1, init_gold, init_silver, merge_to_silver):
    init_silver()
    init_gold()
    silver_table = merge_to_silver(batch_1)
    return (silver_table,)


@app.cell
def _(silver_table):
    silver_table.history()
    return


@app.cell
def _(conn, mo, silver_estimates):
    _df = mo.sql(
        """
        select * from silver_estimates
        """,
        engine=conn,
    )
    return


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
    )
    return (batch_2,)


@app.cell
def _(append_bronze, batch_2, merge_to_silver):
    append_bronze(batch_2)
    merge_to_silver(batch_2)
    return


@app.cell
def _(bronze_estimates, conn, mo):
    _df = mo.sql(
        """
        select * from bronze_estimates
        """,
        engine=conn,
    )
    return


@app.cell
def _(conn, mo, silver_estimates):
    _df = mo.sql(
        """
        select * from silver_estimates;
        """,
        engine=conn,
    )
    return


@app.cell
def _(dt, make_batch):
    batch_3 = make_batch(
        [
            # Mutate estimate for the exising
            ("M", str(1).zfill(4), 2026, dt.date(2026, 1, 1), "USD", 9999.0),
            # add a new estimate for the same business context April
            ("M", str(1).zfill(4), 2026, dt.date(2026, 4, 1), "USD", 3100.0),
            # mutate existing annual estimate
            ("A", str(4).zfill(4), 2026, None, "USD", 9999.0),
        ],
    )
    return (batch_3,)


@app.cell
def _(append_bronze, batch_3, merge_to_silver):
    append_bronze(batch_3)
    merge_to_silver(batch_3)
    return


@app.cell
def _(bronze_estimates, conn, mo):
    _df = mo.sql(
        """
        select * from bronze_estimates
        """,
        engine=conn,
    )
    return


@app.cell
def _(conn, mo, silver_estimates):
    _df = mo.sql(
        """
        SELECT * from silver_estimates
        """,
        engine=conn,
    )
    return


@app.cell
def _(append_bronze, dt, make_batch, merge_to_silver):
    # add new estimates monthly for the given context
    batch_m = make_batch(
        [
            ("M", str(99).zfill(4), 2026, dt.date(2026, 1, 1), "USD", 9999.0),
            ("M", str(99).zfill(4), 2026, dt.date(2026, 4, 1), "USD", 9999.0),
        ]
    )

    # test M -> A change
    batch_a = make_batch(
        [
            ("A", str(99).zfill(4), 2026, None, "USD", 1000.0),
        ]
    )

    append_bronze(batch_m)
    merge_to_silver(batch_m)

    append_bronze(batch_a)
    merge_to_silver(batch_a)
    return


@app.cell
def _(bronze_estimates, conn, mo):
    _df = mo.sql(
        """
        select * from bronze_estimates
        """,
        engine=conn,
    )
    return


@app.cell
def _(conn, mo, silver_estimates):
    _df = mo.sql(
        """
        select * from silver_estimates
        """,
        engine=conn,
    )
    return


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
    return


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
    return


@app.cell
def _(init_gold, start_gold_pipeline):
    init_gold()
    query = start_gold_pipeline()
    return


@app.cell
def _(gold_path, spark):
    gold_df = spark.read.format("delta").load(gold_path).orderBy("account_id", "month")
    gold_df.show(truncate=False)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
