import marimo

__generated_with = "0.21.1"
app = marimo.App(width="columns")


@app.cell(column=0)
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
def _(conn):
    conn.list_tables()
    return


@app.cell
def _(dt, make_estimate_batch, make_ledger_batch):
    # Batch 1: Two accounts in Annual mode for 2026
    batch_1 = make_estimate_batch(
        [
            ("A", str(1).zfill(4), 2026, None, "USD", 12000.0),
            ("A", str(2).zfill(4), 2026, None, "USD", 24000.0),
        ],
    )

    # Mock Ledger for Account 1: 3 months at $800/month = $2400 total
    # This leaves $9600 for 9 remaining months = $1066.67/month
    ledger_batch_1 = make_ledger_batch(
        [
            (str(1).zfill(4), 2026, dt.date(2026, 1, 1), 800.0, "Cedent"),
            (str(1).zfill(4), 2026, dt.date(2026, 2, 1), 800.0, "Cedent"),
            (str(1).zfill(4), 2026, dt.date(2026, 3, 1), 800.0, "Cedent"),
        ]
    )

    # Mock Ledger for Account 2: 3 months at $1500/month = $4500 total
    # This leaves $19500 for 9 remaining months = $2166.67/month
    ledger_batch_2 = make_ledger_batch(
        [
            (str(2).zfill(4), 2026, dt.date(2026, 1, 1), 1500.0, "Cedent"),
            (str(2).zfill(4), 2026, dt.date(2026, 2, 1), 1500.0, "Cedent"),
            (str(2).zfill(4), 2026, dt.date(2026, 3, 1), 1500.0, "Cedent"),
        ]
    )
    return batch_1, ledger_batch_1, ledger_batch_2


@app.cell
def _(
    append_bronze_estimates,
    append_bronze_ledger,
    batch_1,
    init_bronze_estimates,
    init_bronze_ledger,
    ledger_batch_1,
    ledger_batch_2,
):
    # Initialize and populate Bronze tables
    init_bronze_estimates()
    init_bronze_ledger()

    append_bronze_estimates(batch_1)
    append_bronze_ledger(ledger_batch_1)
    append_bronze_ledger(ledger_batch_2)
    return


@app.cell
def _(DeltaTable, F, Window, silver_estimate_path, spark):
    def merge_estimates_to_silver(batch_df, batch_id):
        """Merge a micro-batch of bronze estimate changes into silver (monthly granularity).

        Within a batch, the latest submission per account/year determines the authoritative mode:
        - If the latest is Annual: expand to all 12 months, ignore monthly entries in the batch
        - If the latest is Monthly: apply only the specified months, ignore annual entries

        Performance notes:
        - batch_df is consumed once via broadcast join to avoid multiple scans
        - Silver table merge performance depends on partitioning; ensure the silver
          table is partitioned by (year, month) for partition pruning on merge
        """

        now = F.current_timestamp()
        account_window = Window.partitionBy("account_id", "year").orderBy(F.desc("_inserted_at"))

        # Step 1: Determine authoritative mode per account/year using a single window.
        # The result set is tiny relative to the batch, so broadcast it.
        auth_mode = (
            batch_df
            .withColumn("rn", F.row_number().over(account_window))
            .filter(F.col("rn") == 1)
            .select("account_id", "year", F.col("estimation_mode").alias("auth_mode"))
        )

        # Tag every row in the batch with its account/year authoritative mode.
        # Broadcast eliminates a shuffle since auth_mode is small.
        tagged = batch_df.join(F.broadcast(auth_mode), ["account_id", "year"])

        # Step 2: For accounts where latest is Annual - expand to 12 months.
        # Filter the already-tagged batch in-memory; no re-scan of batch_df.
        annual_data = (
            tagged.filter(
                (F.col("auth_mode") == "A") & (F.col("estimation_mode") == "A")
            )
            .withColumn("rn", F.row_number().over(account_window))
            .filter(F.col("rn") == 1)
            .drop("rn", "auth_mode")
            .select("account_id", "year", "currency", "estimate", "_event_id")
        )

        months_df = spark.range(12).withColumnRenamed("id", "month_idx")
        annual_monthly = (
            annual_data
            .crossJoin(months_df)
            .withColumn("month", F.make_date(F.col("year"), F.col("month_idx") + 1, F.lit(1)))
            .withColumn("estimation_mode", F.lit("A"))
            .withColumn("estimate", F.col("estimate") / 12)
            .drop("month_idx")
        )

        # Step 3: For accounts where latest is Monthly - take latest per month.
        monthly_window = Window.partitionBy("account_id", "year", "month").orderBy(F.desc("_inserted_at"))
        monthly_data = (
            tagged.filter(
                (F.col("auth_mode") == "M") & (F.col("estimation_mode") == "M")
            )
            .withColumn("rn", F.row_number().over(monthly_window))
            .filter(F.col("rn") == 1)
            .drop("rn", "auth_mode")
            .select("account_id", "year", "month", "currency", "estimate", "_event_id", "estimation_mode")
        )

        # Combine and merge
        all_estimates = (
            annual_monthly.unionByName(monthly_data)
            .withColumn("_updated_at", now)
            .drop("_change_type", "_commit_version", "_commit_timestamp", "_inserted_at", "_batch_id")
        )

        silver_dt = DeltaTable.forPath(spark, silver_estimate_path)

        (
            silver_dt.alias("target")
            .merge(
                all_estimates.alias("source"),
                "target.account_id = source.account_id "
                "AND target.year = source.year "
                "AND target.month = source.month",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    return (merge_estimates_to_silver,)


@app.cell
def _(DeltaTable, F, silver_ledger_path, spark):
    def merge_ledger_to_silver(batch_df, batch_id):
        """Merge a micro-batch of bronze ledger changes into silver.

        Designed for use with Spark Streaming foreachBatch.
        """

        bronze_df = (
            batch_df
            .drop("_inserted_at", "_batch_id", "_change_type", "_commit_version", "_commit_timestamp")
            .groupBy("account_id", "year", "month", "type")
            .agg(
                F.sum("amount").alias("amount"),
                F.max("_event_id").alias("_event_id"),
            )
            .withColumn("_updated_at", F.current_timestamp())
        )

        silver_dt = DeltaTable.forPath(spark, silver_ledger_path)

        (
            silver_dt.alias("target")
            .merge(
                bronze_df.alias("source"),
                "target.account_id = source.account_id "
                "AND target.year = source.year "
                "AND target.month = source.month "
                "AND target.type = source.type",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    return (merge_ledger_to_silver,)


@app.cell
def _(init_silver_control, init_silver_estimates, init_silver_ledger):
    # Initialize all silver tables
    init_silver_estimates()
    init_silver_ledger()
    init_silver_control()
    return


@app.cell
def _(append_bronze_estimates, dt, make_estimate_batch):
    # Initialize and populate Bronze tables
    monthly_batch = make_estimate_batch(
        [
            ("M", str(1).zfill(4), 2026, dt.date(2026, 1, 1), "USD", 5000.0),
            ("M", str(1).zfill(4), 2026, dt.date(2026, 2, 1), "USD", 5000.0),
            ("M", str(1).zfill(4), 2026, dt.date(2026, 3, 1), "USD", 5000.0),
            ("M", str(1).zfill(4), 2026, dt.date(2026, 1, 1), "USD", 1000.0),
            ("M", str(1).zfill(4), 2026, dt.date(2026, 2, 1), "USD", 1000.0),
        ],
    )

    append_bronze_estimates(monthly_batch)
    return


@app.cell
def _(conn, mo, silver_estimates):
    _df = mo.sql(
        f"""
        select * from silver_estimates
        """,
        engine=conn
    )
    return


@app.cell
def _(append_bronze_estimates, dt, make_estimate_batch):
    m_batch = make_estimate_batch(
        [
            ("M", str(1).zfill(4), 2026, dt.date(2026, 4, 1), "USD", 5000.0),
        ],
    )

    append_bronze_estimates(m_batch)

    y_batch = make_estimate_batch(
        [
            ("A", str(1).zfill(4), 2026, None, "USD", 12000.0),
        ],
    )

    append_bronze_estimates(y_batch)
    return


@app.cell
def _(append_bronze_estimates, dt, make_estimate_batch):
    m_batch_2 = make_estimate_batch(
        [
            ("M", str(1).zfill(4), 2026, dt.date(2026, 12, 1), "USD", 999.0),
        ],
    )

    append_bronze_estimates(m_batch_2)
    return


@app.cell
def poll_bronze_via_cdf(
    bronze_estimate_path,
    bronze_ledger_path,
    merge_estimates_to_silver,
    merge_ledger_to_silver,
    spark,
):
    # Poll bronze via CDF and stream into silver
    est_query = (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .load(bronze_estimate_path)
        .writeStream.foreachBatch(merge_estimates_to_silver)
        .option("checkpointLocation", "s3a://lakehouse/delta/checkpoints/silver_estimates")
        .trigger(availableNow=True)
        .start()
    )

    ledger_query = (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .load(bronze_ledger_path)
        .writeStream.foreachBatch(merge_ledger_to_silver)
        .option("checkpointLocation", "s3a://lakehouse/delta/checkpoints/silver_ledger")
        .trigger(availableNow=True)
        .start()
    )

    # Wait for both to complete
    est_query.awaitTermination()
    ledger_query.awaitTermination()
    return (est_query,)


@app.cell
def _(est_query):
    dir(est_query)
    return


@app.cell
def _(est_query):
    est_query.lastProgress
    return


@app.cell
def _(append_control_event, dt):
    # Seed initial holdback dates for our test accounts
    append_control_event(str(1).zfill(4), 2026, dt.date(2026, 3, 31))
    append_control_event(str(2).zfill(4), 2026, dt.date(2026, 3, 31))
    return


@app.cell
def _(bronze_estimates, conn, mo):
    _df = mo.sql(
        f"""
        select * from bronze_estimates where account_id = '0002' order by _inserted_at desc
        """,
        engine=conn
    )
    return


@app.cell
def _(conn, mo, silver_estimates):
    _df = mo.sql(
        f"""
        select * from silver_estimates where account_id = '0002' order by _updated_at desc
        """,
        engine=conn
    )
    return


@app.cell
def _(conn, gold_transactional_ledger, mo):
    _df = mo.sql(
        f"""
        select * from gold_transactional_ledger where account_id = '0002' order by underwriting_month, _inserted_at DESC
        """,
        engine=conn
    )
    return


@app.cell
def _(F, uuid):
    def delta_calculation_engine(estimates_df, hbd_df, ledger_df):
        """
        Set-based delta calculation.

        Silver estimates already contain the final monthly values.
        Apply holdback date boundary (closed months -> target = 0), then compute
        delta against existing ledger.
        """
        now = F.current_timestamp()

        # 1. Apply HBD boundary: closed months get target=0, open months keep estimate
        with_targets = (
            estimates_df
            .join(hbd_df, ["account_id", "year"], "left")
            .filter(
                F.col("holdback_date").isNull() | (F.col("month") > F.col("holdback_date"))
            )
            .withColumnRenamed(
                "estimate",
                "target",
            )
        )

        # 2. Fetch existing ledger sums
        existing_sums = (
            ledger_df
            .filter(F.col("category") == "estimate")
            .groupBy("account_id", "year", "underwriting_month")
            .agg(F.sum("amount").alias("existing"))
            .withColumnRenamed("underwriting_month", "month")
        )

        # 3. Compute deltas: delta = target - existing
        deltas = (
            with_targets
            .join(existing_sums, ["account_id", "year", "month"], "left")
            .withColumn("existing", F.coalesce(F.col("existing"), F.lit(0.0)))
            .withColumn("delta", F.col("target") - F.col("existing"))
        )

        # 4. Format as journal entries
        journal_entries = (
            deltas
            .select(
                "account_id",
                "year",
                F.col("month").alias("underwriting_month"),
                F.col("delta").alias("amount"),
                F.lit("estimate").alias("category"),
                F.lit("estimate").alias("actual_type").cast("string"),
                F.coalesce(F.col("currency"), F.lit("USD")).alias("currency"),
                F.col("_event_id").alias("_source_event_id"),
                F.lit(str(uuid.uuid4())).alias("_journal_event_id"),
                now.alias("_inserted_at")
            )
            .filter(F.col("amount") > F.lit(0.0))
        )

        return journal_entries

    return (delta_calculation_engine,)


@app.cell
def _(delta_calculation_engine, spark):
    # Read full silver state and existing ledger
    estimates_df = spark.table("silver_estimates")
    hbd_df = spark.table("silver_control")
    ledger_df = spark.table("gold_transactional_ledger")

    # Compute deltas
    deltas = delta_calculation_engine(estimates_df, hbd_df, ledger_df)
    deltas
    return


@app.cell
def _(F, Window, delta_calculation_engine, gold_txn_ledger_path, spark):
    app_id = "streaming-transactional-ledger"


    def process_silver_estimates_batch(batch_df, batch_id):
        """
        Process micro-batch of silver_estimates CDF changes into gold transactional ledger.

        Deduplicates within the micro-batch to get latest value per account/year/month,
        then computes and books deltas.
        """

        if batch_df.isEmpty():
            print(f"[Batch {batch_id}] Empty batch, skipping")
            return

        print(f"\n[Batch {batch_id}] === RAW BATCH ===")
        print(f"[Batch {batch_id}] Total rows: {batch_df.count()}")
        print(f"[Batch {batch_id}] Change types:")
        batch_df.groupBy("_change_type").count().show()
        print(f"[Batch {batch_id}] Commit versions:")
        batch_df.select("_commit_version").distinct().show()
        print(f"[Batch {batch_id}] Sample rows:")
        batch_df.show(10, truncate=False)

        # 1. Deduplicate: get latest post-image per account/year/month in batch
        deduped_batch = (
            batch_df.filter(
                F.col("_change_type").isin(["insert", "update_postimage"])
            )
            .withColumn(
                "rn",
                F.row_number().over(
                    Window.partitionBy("account_id", "year", "month").orderBy(
                        F.desc("_updated_at")
                    )
                ),
            )
            .filter(F.col("rn") == 1)
            .drop("rn", "_change_type", "_commit_version", "_commit_timestamp")
        )

        print(f"\n[Batch {batch_id}] === AFTER DEDUP ===")
        print(f"[Batch {batch_id}] Deduped count: {deduped_batch.count()}")
        deduped_batch.show(24, truncate=False)

        # 2. Get latest HBD per account/year
        hbd_df = spark.table("silver_control")
        latest_hbd = (
            hbd_df.withColumn(
                "rn",
                F.row_number().over(
                    Window.partitionBy("account_id", "year").orderBy(
                        F.desc("_inserted_at")
                    )
                ),
            )
            .filter(F.col("rn") == 1)
            .select("account_id", "year", "holdback_date")
        )

        print(f"\n[Batch {batch_id}] === HBD ===")
        latest_hbd.show()

        # 3. Get existing ledger
        ledger_df = spark.table("gold_transactional_ledger")

        print(f"\n[Batch {batch_id}] === LEDGER ===")
        print(f"[Batch {batch_id}] Ledger count: {ledger_df.count()}")
        ledger_df.show(24, truncate=False)

        # 4. Compute deltas
        deltas = delta_calculation_engine(deduped_batch, latest_hbd, ledger_df)

        print(f"\n[Batch {batch_id}] === DELTAS ===")
        print(f"[Batch {batch_id}] Delta count: {deltas.count()}")
        deltas.show(24, truncate=False)

        deltas.write.format("delta").option("txnVersion", batch_id).option(
            "txnAppId", app_id
        ).mode("append").save(gold_txn_ledger_path)
        print(f"\n[Batch {batch_id}] === WRITTEN TO GOLD ===")

    return (process_silver_estimates_batch,)


@app.cell
def _(process_silver_estimates_batch, silver_estimate_path, spark):
    # Trigger 1: Stream silver_estimates CDF -> gold transactional ledger
    est_stream = (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .load(silver_estimate_path)
        .writeStream.foreachBatch(process_silver_estimates_batch)
        .option("checkpointLocation", "s3a://lakehouse/delta/checkpoints/gold_estimates")
        .trigger(availableNow=True)
        .start()
    )

    est_stream.awaitTermination()
    return


@app.cell
def _(process_silver_estimates_batch, silver_estimate_path, spark):
    # Trigger 1: Stream silver_estimates CDF -> gold transactional ledger
    est_stream_2 = (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .load(silver_estimate_path)
        .writeStream.foreachBatch(process_silver_estimates_batch)
        .option("checkpointLocation", "s3a://lakehouse/delta/checkpoints/gold_estimates")
        .trigger(availableNow=True)
        .start()
    )

    est_stream_2.awaitTermination()
    return


@app.cell
def _(append_bronze_estimates, make_estimate_batch):
    batch_dup = make_estimate_batch(
        [
            ("A", str(2).zfill(4), 2026, None, "USD", 36.0),
        ],
    )

    append_bronze_estimates(batch_dup)

    batch_a = make_estimate_batch(
        [
            ("A", str(2).zfill(4), 2026, None, "USD", 48.0),
        ],
    )

    append_bronze_estimates(batch_a)
    return


@app.cell
def _(spark):
    spark.read.json("s3a://lakehouse/delta/checkpoints/gold_estimates/offsets/*").show(truncate=False)
    return


@app.cell
def _(spark):
    spark.sql("DESCRIBE HISTORY silver_estimates").show()
    return


@app.cell
def _(spark):
    print("=== Current Silver Estimates ===")
    spark.table("silver_estimates").orderBy("account_id", "year", "month").show(24, truncate=False)
    return


@app.cell
def _(spark):
    print("=== Current Gold Ledger ===")
    spark.table("gold_transactional_ledger").orderBy("account_id", "year", "underwriting_month", "_updated_at").show(100, truncate=False)
    return


@app.cell
def _(spark):
    print("=== Current Bronze Estimates ===")
    spark.table("bronze_estimates").orderBy("account_id", "year", "_inserted_at").show(100, truncate=False)
    return


@app.cell
def _(spark):
    print("=== Current Silver Control ===")
    spark.table("silver_control").show()
    return


@app.cell(column=1)
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
    from pyspark.sql.window import Window

    sys.path.insert(0, str(Path(__file__).parent))

    from config import create_delta_spark_session, get_s3_store

    return (
        DeltaTable,
        F,
        T,
        Window,
        create_delta_spark_session,
        dt,
        get_s3_store,
        mo,
        random,
        uuid,
    )


@app.cell
def _(T):
    # --- Estimate Schemas (existing) ---
    estimate_schema = T.StructType(
        [
            T.StructField("estimation_mode", T.StringType(), False),
            T.StructField("account_id", T.StringType(), False),
            T.StructField("year", T.IntegerType(), False),
            T.StructField("month", T.DateType(), True),  # null for annual in bronze
            T.StructField("currency", T.StringType(), False),
            T.StructField("estimate", T.DoubleType(), False),
        ]
    )

    bronze_estimate_schema = T.StructType(
        [
            *estimate_schema.fields,
            T.StructField("_inserted_at", T.TimestampType(), True),
            T.StructField("_batch_id", T.StringType(), False),
            T.StructField("_event_id", T.StringType(), False),
        ]
    )
    # Silver always stores monthly granularity (month is never null)
    silver_estimate_schema = T.StructType(
        [
            T.StructField("estimation_mode", T.StringType(), False),
            T.StructField("account_id", T.StringType(), False),
            T.StructField("year", T.IntegerType(), False),
            T.StructField("month", T.DateType(), False),
            T.StructField("currency", T.StringType(), False),
            T.StructField("estimate", T.DoubleType(), False),
            T.StructField("_updated_at", T.TimestampType(), False),
            T.StructField("_event_id", T.StringType(), False),
        ]
    )

    # --- Ledger Schemas (new) ---
    ledger_schema = T.StructType(
        [
            T.StructField("account_id", T.StringType(), False),
            T.StructField("year", T.IntegerType(), False),
            T.StructField("month", T.DateType(), False),
            T.StructField("amount", T.DoubleType(), False),
            T.StructField("type", T.StringType(), False),
        ]
    )

    bronze_ledger_schema = T.StructType(
        [
            *ledger_schema.fields,
            T.StructField("_inserted_at", T.TimestampType(), True),
            T.StructField("_batch_id", T.StringType(), False),
            T.StructField("_event_id", T.StringType(), False),
        ]
    )
    silver_ledger_schema = T.StructType(
        [
            *ledger_schema.fields,
            T.StructField("_updated_at", T.TimestampType(), False),
            T.StructField("_event_id", T.StringType(), False),
        ]
    )


    transactional_ledger_schema = T.StructType(
        [
            T.StructField("account_id", T.StringType(), False),
            T.StructField("year", T.IntegerType(), False),
            T.StructField("underwriting_month", T.DateType(), False),
            T.StructField("amount", T.DoubleType(), False),
            T.StructField("category", T.StringType(), False),
            T.StructField("actual_type", T.StringType(), True),
            T.StructField("currency", T.StringType(), True),
            T.StructField("_source_event_id", T.StringType(), False),
            T.StructField("_journal_event_id", T.StringType(), False),
            T.StructField("_inserted_at", T.TimestampType(), False),
        ]
    )

    # --- Control Table for Holdback Date events Schema ---
    control_schema = T.StructType(
        [
            T.StructField("account_id", T.StringType(), False),
            T.StructField("year", T.IntegerType(), False),
            T.StructField("holdback_date", T.DateType(), False),
            T.StructField("_inserted_at", T.TimestampType(), False),
            T.StructField("_holdback_event_id", T.StringType(), False),
        ]
    )
    return (
        bronze_estimate_schema,
        bronze_ledger_schema,
        control_schema,
        silver_estimate_schema,
        silver_ledger_schema,
        transactional_ledger_schema,
    )


@app.cell
def _(DeltaTable, silver_estimate_schema, spark):
    silver_estimate_path = "s3a://lakehouse/delta/silver_estimates"

    def init_silver_estimates():
        DeltaTable.createIfNotExists(spark).tableName("silver_estimates").location(
            silver_estimate_path
        ).addColumns(silver_estimate_schema).property(
            "delta.enableChangeDataFeed", "true"
        ).execute()
        return silver_estimate_path


    return init_silver_estimates, silver_estimate_path


@app.cell
def _(DeltaTable, F, bronze_estimate_schema, bronze_ledger_schema, spark):
    bronze_estimate_path = "s3a://lakehouse/delta/bronze_estimates"
    bronze_ledger_path = "s3a://lakehouse/delta/bronze_ledger"

    def init_bronze_estimates():
        DeltaTable.createIfNotExists(spark).location(bronze_estimate_path).tableName(
            "bronze_estimates"
        ).addColumns(bronze_estimate_schema).property(
            "delta.enableChangeDataFeed", "true"
        ).execute()
        return bronze_estimate_path

    def init_bronze_ledger():
        DeltaTable.createIfNotExists(spark).location(bronze_ledger_path).tableName(
            "bronze_ledger"
        ).addColumns(bronze_ledger_schema).property(
            "delta.enableChangeDataFeed", "true"
        ).execute()
        return bronze_ledger_path

    def append_bronze_estimates(df):
        df.withColumn("_inserted_at", F.current_timestamp()).write.format("delta").mode(
            "append"
        ).save(bronze_estimate_path)
        return bronze_estimate_path

    def append_bronze_ledger(df):
        df.withColumn("_inserted_at", F.current_timestamp()).write.format("delta").mode(
            "append"
        ).save(bronze_ledger_path)
        return bronze_ledger_path

    return (
        append_bronze_estimates,
        append_bronze_ledger,
        bronze_estimate_path,
        bronze_ledger_path,
        init_bronze_estimates,
        init_bronze_ledger,
    )


@app.cell
def _(DeltaTable, silver_ledger_schema, spark):
    silver_ledger_path = "s3a://lakehouse/delta/silver_ledger"

    def init_silver_ledger():
        try:
            DeltaTable.createIfNotExists(spark).tableName("silver_ledger").location(
                silver_ledger_path
            ).addColumns(silver_ledger_schema).property(
                "delta.enableChangeDataFeed", "true"
            ).execute()
        except Exception:
            # Table already exists with different schema, ignore
            raise
        return silver_ledger_path


    return init_silver_ledger, silver_ledger_path


@app.cell
def _(DeltaTable, control_schema, dt, spark, uuid):
    silver_control_path = "s3a://lakehouse/delta/silver_control"

    def init_silver_control():
        DeltaTable.createIfNotExists(spark).tableName("silver_control").location(
            silver_control_path
        ).addColumns(control_schema).execute()
        return silver_control_path

    def append_control_event(account_id, year, holdback_date):
        from pyspark.sql import Row
        row = Row(
            account_id=account_id,
            year=year,
            holdback_date=holdback_date,
            _inserted_at=dt.datetime.now(),
            _holdback_event_id=str(uuid.uuid4()),
        )
        df = spark.createDataFrame([row], schema=control_schema)
        df.write.format("delta").mode("append").save(silver_control_path)
        return silver_control_path

    return append_control_event, init_silver_control


@app.cell
def _(
    T,
    bronze_estimate_schema,
    bronze_ledger_schema,
    dt,
    random,
    spark,
    uuid,
):
    def make_estimate_batch(rows):
        _batch_id = str(random.randint(0, 9999)).zfill(4)
        now = dt.datetime.now()
        data = [(*row, now, _batch_id, str(uuid.uuid4())) for row in rows]
        return spark.createDataFrame(data, schema=bronze_estimate_schema)

    def make_ledger_batch(rows):
        _batch_id = str(random.randint(0, 9999)).zfill(4)
        now = dt.datetime.now()
        data = [(*row, now, _batch_id, str(uuid.uuid4())) for row in rows]
        return spark.createDataFrame(data, schema=bronze_ledger_schema)

    def make_context_df(rows):
        """Create a context DataFrame for testing the set-based interface.

        Args:
            rows: List of tuples (account_id, year, holdback_date, _holdback_event_id, _volume_event_id)
                  where _holdback_event_id and _volume_event_id can be None
        """
        context_schema = T.StructType([
            T.StructField("account_id", T.StringType(), False),
            T.StructField("year", T.IntegerType(), False),
            T.StructField("holdback_date", T.DateType(), False),
            T.StructField("_holdback_event_id", T.TimestampType(), True),
            T.StructField("_volume_event_id", T.StringType(), True),
        ])
        return spark.createDataFrame(rows, schema=context_schema)

    return make_estimate_batch, make_ledger_batch


@app.cell
def _(DeltaTable, spark, transactional_ledger_schema):
    gold_txn_ledger_path = "s3a://lakehouse/delta/gold_transactional_ledger"

    def init_txn_ledger():
        DeltaTable.createIfNotExists(spark).location(
            gold_txn_ledger_path
        ).tableName("gold_transactional_ledger").addColumns(
            transactional_ledger_schema
        ).execute()
        return gold_txn_ledger_path

    init_txn_ledger()
    return (gold_txn_ledger_path,)


@app.cell
def debug_silver_cdf(F, Window, spark):
    # Read all CDF changes from silver_estimates
    def read_all_cdf(table_path_name, starting_version=0):

        cdf_df = (
            spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", starting_version)
            .load(table_path_name)
        )

        print("=== Total CDF rows:", cdf_df.count())
        print("\n=== CDF Schema:")
        cdf_df.printSchema()

        print("\n=== Changes by type and version:")
        cdf_df.groupBy("_change_type", "_commit_version").count().orderBy(
            "_commit_version", "_change_type"
        ).show()

        print("\n=== All changes for account 0002:")
        cdf_df.filter(F.col("account_id") == "0002").orderBy(
            "_commit_version", "month", "_change_type"
        ).show(100, truncate=False)

        print("\n=== Post-images only for account 0002:")
        post_images = cdf_df.filter(
            (F.col("account_id") == "0002")
            & (F.col("_change_type") == "update_postimage")
        )
        post_images.orderBy("_commit_version", "month").show(100, truncate=False)

        print("\n=== Pre-images only for account 0002:")
        pre_images = cdf_df.filter(
            (F.col("account_id") == "0002")
            & (F.col("_change_type") == "update_preimage")
        )
        pre_images.orderBy("_commit_version", "month").show(100, truncate=False)

        print("\n=== Deduplicated (latest per month using row_number):")
        deduped = (
            cdf_df.filter(
                F.col("_change_type").isin(["insert", "update_postimage"])
            )
            .withColumn(
                "rn",
                F.row_number().over(
                    Window.partitionBy("account_id", "year", "month").orderBy(
                        F.desc("_commit_timestamp")
                    )
                ),
            )
            .filter(F.col("rn") == 1)
            .drop("rn")
            .orderBy("month")
        )
        deduped.show(24, truncate=False)

        print("\n=== Commit timestamps:")
        cdf_df.select("_commit_version", "_commit_timestamp").distinct().orderBy(
            "_commit_version"
        ).show()

    return


@app.cell
def _():
    return


@app.cell(column=2, hide_code=True)
def _(mo):
    mo.md(r"""
    Goal

    Implement a event sourced transactional ledger (MVP) via Spark/Delta Table.

    1. User business volume estimates are submitted for a given account for a given
    period in Annual and Monthly granularity estimation modes. This should be saved as
    bronze layer for append only style log.


    ```
        ("A", str(1).zfill(4), 2026, None, "USD", 12000.0)
        ("M", str(1).zfill(4), 2026, dt.date(2026, 1, 1), "USD", 5000.0),
        ("M", str(1).zfill(4), 2026, dt.date(2026, 2, 1), "USD", 5000.0),
        ("M", str(1).zfill(4), 2026, dt.date(2026, 3, 1), "USD", 5000.0),
        ("M", str(1).zfill(4), 2026, dt.date(2026, 1, 1), "USD", 1000.0),
        ("M", str(1).zfill(4), 2026, dt.date(2026, 2, 1), "USD", 1000.0),
    ```

    - these specify the user intent value for the given account given financial period
      - these are snapshots
    - the latest snapshot is always the authoritative truth
    - the trasactional ledger always have monthly granularity
    - this impacts downstream (ledger) if the user submission is "A" (annual) the volume estimates have
      to be redistributed for that give financial period from the holdback_date
      (annual_estimation_value) / (remaining months from holdback_date to end of the
      year)
    - there can be use case where two different users submit the same estimates - how to
      handle this?

    - Mode Switches: A↔M; user can swith from Annual to Monthly estimations modes. If
      the account started with Annual then the user submits and monthly we need to
      balance the books on the ledger:

    Initial volume estimate:
    ("A", str(1).zfill(4), 2026, None, "USD", 12000.0)

    this means in the ledger we need to create perhaps 12 months of

    ("M", str(1).zfill(4), 2026, dt.date(2026, 1, 1), "USD", +1000.0),

    if later then user submits a new estimate

    ("M", str(1).zfill(4), 2026, dt.date(2026, 1, 1), "USD", 1500.0),

    we can perhaps create a temp view or just fetch that existing account in the ledger
    as a dataframe multiply by `-1` and union by the incoming new value therefore we get
    the delta and we book this value in the ledger

    existing account multipied by -1.0
    ("M", str(1).zfill(4), 2026, dt.date(2026, 1, 1), "USD", -1000.0),
    union with the incoming account:
    ("M", str(1).zfill(4), 2026, dt.date(2026, 1, 1), "USD", 1500.0),
    this will result in a
    ("M", str(1).zfill(4), 2026, dt.date(2026, 1, 1), "USD", 500.0),

    which will be booked on the transactinal ledger - there for the accounts are
    balancing.

    If the accounts have been in Monthly estimation mode and then swithching to annual,
    we need to perform the same operation. Redistribute the incoming annual based on the
    holdback_date, compute the deltas for each month and book those deltas in the
    ledger.


    HBD (holdback_date): drives estimates, accruals (cedent) and reconsiliations.
    - cannot be set beyond current financial period end
    - moves forward or backward
    - moving holdback_date backward (for late incoming bookings) triggers
      re-distribution of future estimates to earlier periods (annual mode only)
    - moving holdback_date forward typically occurs once bookings are closed.

    An example of the transactional_ledger with a subset of columns:

    account_id	year	year_month	ammount	category	new_estimates	new_bookings
    1	2026	2026-01-01	1,000.00	estimates	0.00	-1,000.00
    1	2026	2026-01-02	1,000.00	estimates	0.00	-1,000.00
    1	2026	2026-01-03	1,000.00	estimates	0.00	-1,000.00
    1	2026	2026-01-04	1,000.00	estimates	1,333.33	333.33
    1	2026	2026-01-05	1,000.00	estimates	1,333.33	333.33
    1	2026	2026-01-06	1,000.00	estimates	1,333.33	333.33
    1	2026	2026-01-07	1,000.00	estimates	1,333.33	333.33
    1	2026	2026-01-08	1,000.00	estimates	1,333.33	333.33
    1	2026	2026-01-09	1,000.00	estimates	1,333.33	333.33
    1	2026	2026-01-10	1,000.00	estimates	1,333.33	333.33
    1	2026	2026-01-11	1,000.00	estimates	1,333.33	333.33
    1	2026	2026-01-12	1,000.00	estimates	1,333.33	333.33
    Hold back date moved to 2026-03-31			Cedent Data has arrived
    1	2026	2026-01-03	3,000.00	Cedent
    Recalculate estimates and book deltas
    1	2026	2026-01-01	-1,000.00	estimates
    1	2026	2026-01-02	-1,000.00	estimates
    1	2026	2026-01-03	-1,000.00	estimates
    1	2026	2026-01-04	333.33	estimates
    1	2026	2026-01-05	333.33	estimates
    1	2026	2026-01-06	333.33	estimates
    1	2026	2026-01-07	333.33	estimates
    1	2026	2026-01-08	333.33	estimates
    1	2026	2026-01-09	333.33	estimates
    1	2026	2026-01-10	333.33	estimates
    1	2026	2026-01-11	333.33	estimates
    1	2026	2026-01-12	333.33	estimates


    - So we have to book changes in the ledger either when listening to changes of user
      input submissions aka New business volumes estimates in Annual or month mode.
    - book changes when the Cedent data has arrived and the hold back date has moved

    4. Follows Delta Lake Medallion Architecture (Bronze → Silver → Gold)
    """)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
