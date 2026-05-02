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
    # --- Estimate Schemas (existing) ---
    estimate_schema = T.StructType(
        [
            T.StructField("estimation_mode", T.StringType(), False),
            T.StructField("account_id", T.StringType(), False),
            T.StructField("year", T.IntegerType(), False),
            T.StructField("month", T.DateType(), True),
            T.StructField("currency", T.StringType(), False),
            T.StructField("estimate", T.DoubleType(), False),
        ]
    )

    bronze_estimate_schema = T.StructType(
        [
            *estimate_schema.fields,
            T.StructField("inserted_at", T.TimestampType(), True),
            T.StructField("batch_id", T.StringType(), False),
            T.StructField("event_id", T.StringType(), False),
        ]
    )
    silver_estimate_schema = T.StructType(
        [
            *estimate_schema.fields,
            T.StructField("updated_at", T.TimestampType(), False),
            T.StructField("event_id", T.StringType(), False),
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
            T.StructField("inserted_at", T.TimestampType(), True),
            T.StructField("batch_id", T.StringType(), False),
            T.StructField("event_id", T.StringType(), False),
        ]
    )
    silver_ledger_schema = T.StructType(
        [
            *ledger_schema.fields,
            T.StructField("updated_at", T.TimestampType(), False),
            T.StructField("event_id", T.StringType(), False),
        ]
    )

    # --- Gold Schema (enhanced) ---
    gold_schema = T.StructType(
        [
            T.StructField("account_id", T.StringType(), False),
            T.StructField("year", T.IntegerType(), False),
            T.StructField("month", T.DateType(), False),
            T.StructField("holdback_date", T.DateType(), False),
            T.StructField("amount", T.DoubleType(), False),
            T.StructField("is_settled", T.BooleanType(), False),
            T.StructField("source", T.StringType(), False),
            T.StructField("source_quality", T.StringType(), False),
            T.StructField("last_calculated_at", T.TimestampType(), False),
            T.StructField("model_version", T.StringType(), False),
        ]
    )

    # --- Control Table Schema ---
    control_schema = T.StructType(
        [
            T.StructField("account_id", T.StringType(), False),
            T.StructField("year", T.IntegerType(), False),
            T.StructField("holdback_date", T.DateType(), False),
            T.StructField("updated_at", T.TimestampType(), False),
        ]
    )
    return (
        bronze_estimate_schema,
        bronze_ledger_schema,
        control_schema,
        gold_schema,
        silver_estimate_schema,
        silver_ledger_schema,
    )


@app.cell
def _(bronze_estimate_schema, bronze_ledger_schema, random, spark, uuid):
    def make_estimate_batch(rows):
        batch_id = str(random.randint(0, 9999)).zfill(4)
        data = [(*row, None, batch_id, str(uuid.uuid4())) for row in rows]
        return spark.createDataFrame(data, schema=bronze_estimate_schema)

    def make_ledger_batch(rows):
        batch_id = str(random.randint(0, 9999)).zfill(4)
        data = [(*row, None, batch_id, str(uuid.uuid4())) for row in rows]
        return spark.createDataFrame(data, schema=bronze_ledger_schema)

    return make_estimate_batch, make_ledger_batch


@app.cell
def _(make_estimate_batch):
    # Batch 1: Two accounts in Annual mode for 2026
    batch_1 = make_estimate_batch(
        [
            ("A", str(1).zfill(4), 2026, None, "USD", 12000.0),
            ("A", str(2).zfill(4), 2026, None, "USD", 24000.0),
        ],
    )
    return (batch_1,)


@app.cell
def _(dt, make_ledger_batch):
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
            (str(2).zfill(4), 2026, dt.date(2026, 2, 1), 1500.0, "Adjustment"),
            (str(2).zfill(4), 2026, dt.date(2026, 3, 1), 1500.0, "Accrual"),
        ]
    )
    return ledger_batch_1, ledger_batch_2


@app.cell
def _(DeltaTable, F, bronze_estimate_schema, bronze_ledger_schema, spark):
    bronze_estimate_path = "s3a://lakehouse/delta/bronze_estimates"
    bronze_ledger_path = "s3a://lakehouse/delta/bronze_ledger"

    def init_bronze_estimates():
        DeltaTable.createIfNotExists(spark).location(bronze_estimate_path).tableName(
            "bronze_estimates"
        ).addColumns(bronze_estimate_schema).execute()
        return bronze_estimate_path

    def init_bronze_ledger():
        DeltaTable.createIfNotExists(spark).location(bronze_ledger_path).tableName(
            "bronze_ledger"
        ).addColumns(bronze_ledger_schema).execute()
        return bronze_ledger_path

    def append_bronze_estimates(df):
        df.withColumn("inserted_at", F.current_timestamp()).write.format("delta").mode(
            "append"
        ).save(bronze_estimate_path)
        return bronze_estimate_path

    def append_bronze_ledger(df):
        df.withColumn("inserted_at", F.current_timestamp()).write.format("delta").mode(
            "append"
        ).save(bronze_ledger_path)
        return bronze_ledger_path

    return (
        append_bronze_estimates,
        append_bronze_ledger,
        init_bronze_estimates,
        init_bronze_ledger,
    )


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
def _(DataFrame, DeltaTable, F, silver_estimate_schema, spark):
    silver_estimate_path = "s3a://lakehouse/delta/silver_estimates"

    def init_silver_estimates():
        DeltaTable.createIfNotExists(spark).tableName("silver_estimates").location(
            silver_estimate_path
        ).addColumns(silver_estimate_schema).property(
            "delta.enableChangeDataFeed", "true"
        ).execute()
        return silver_estimate_path

    def detect_mode_switch(source_df: DataFrame, target_df: DataFrame) -> DataFrame:
        """
        Find existing target (Silver) rows that must be deleted because the
        estimation mode for their business context changed in the incoming batch.
        """
        source_keys = (
            source_df.select("account_id", "year", "currency", "estimation_mode")
            .distinct()
            .withColumnRenamed("estimation_mode", "src_mode")
        )
        return (
            target_df.join(source_keys, ["account_id", "year", "currency"], "inner")
            .filter(F.col("estimation_mode") != F.col("src_mode"))
            .drop("src_mode")
        )

    def merge_estimates_to_silver(batch_df):
        """
        Atomically merge one batch into Silver using tombstones for mode switches.
        """
        contexts = batch_df.select("account_id", "year", "currency").distinct()

        silver_existing = (
            spark.read.format("delta")
            .load(silver_estimate_path)
            .join(contexts, ["account_id", "year", "currency"])
        )

        switch_rows = detect_mode_switch(batch_df, silver_existing)
        tombstones = switch_rows.withColumn("_delete_flag", F.lit(True))
        incoming = batch_df.withColumn("_delete_flag", F.lit(False))
        merge_source = tombstones.unionByName(incoming, allowMissingColumns=True)

        delta_table = DeltaTable.forPath(spark, silver_estimate_path)
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

    return (
        init_silver_estimates,
        merge_estimates_to_silver,
        silver_estimate_path,
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

    def merge_ledger_to_silver(batch_df):
        """
        Atomically merge ledger data into Silver.
        Deduplicates on account_id + year + month + type.
        """
        # Add updated_at timestamp if not present
        from pyspark.sql import functions as F
        batch_df = batch_df.withColumn("updated_at", F.current_timestamp())

        delta_table = DeltaTable.forPath(spark, silver_ledger_path)
        (
            delta_table.alias("t")
            .merge(
                batch_df.alias("s"),
                """t.account_id = s.account_id
                   AND t.year = s.year
                   AND t.month = s.month
                   AND t.type = s.type""",
            )
            .whenMatchedUpdate(
                set={
                    "amount": "s.amount",
                    "updated_at": "s.updated_at",
                    "event_id": "s.event_id",
                },
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        return delta_table

    return init_silver_ledger, merge_ledger_to_silver, silver_ledger_path


@app.cell
def _(
    batch_1,
    init_silver_estimates,
    init_silver_ledger,
    ledger_batch_1,
    ledger_batch_2,
    merge_estimates_to_silver,
    merge_ledger_to_silver,
):
    # Initialize Silver tables and merge initial data
    init_silver_estimates()
    init_silver_ledger()

    merge_estimates_to_silver(batch_1)
    merge_ledger_to_silver(ledger_batch_1)
    merge_ledger_to_silver(ledger_batch_2)
    return


@app.cell
def _(conn, mo, silver_ledger):
    _df = mo.sql(
        f"""
        select * from silver_ledger
        """,
        engine=conn
    )
    return


@app.cell
def _(
    DataFrame,
    DeltaTable,
    F,
    control_schema,
    dt,
    gold_schema,
    silver_estimate_path,
    silver_ledger_path,
    spark,
):
    gold_path = "s3a://lakehouse/delta/gold_unified_cashflow"
    control_path = "s3a://lakehouse/delta/control_holdback_dates"

    def init_gold():
        DeltaTable.createIfNotExists(spark).tableName("gold_unified_cashflow").location(
            gold_path
        ).addColumns(gold_schema).execute()

    def init_control():
        DeltaTable.createIfNotExists(spark).tableName("control_holdback_dates").location(
            control_path
        ).addColumns(control_schema).execute()

    def calculate_unified_cashflow(
        account_id: str, year: int, holdback_date: dt.date
    ) -> DataFrame:
        """
        Calculate the unified cashflow for a single account at a given holdback date.

        Logic:
        - For months <= holdback: Use ledger actuals (sum of Cedent + Adjustment + Accrual)
        - For months > holdback:
          - Monthly mode: Use the specific monthly estimate
          - Annual mode: Linearly distribute remaining target
        """
        # Read current silver data for this account only
        silver_estimates = (
            spark.read.format("delta")
            .load(silver_estimate_path)
            .filter((F.col("account_id") == account_id) & (F.col("year") == year))
        )
        silver_ledger = (
            spark.read.format("delta")
            .load(silver_ledger_path)
            .filter((F.col("account_id") == account_id) & (F.col("year") == year))
        )

        # Aggregate ledger by account/year/month
        ledger_monthly = (
            silver_ledger.groupBy("account_id", "year", "month")
            .agg(F.sum("amount").alias("ledger_amount"))
        )

        # Get account/year/currency from estimates
        accounts = silver_estimates.select("account_id", "year", "currency").distinct()

        # Generate all months for the year using Spark SQL sequence
        months_df = spark.sql(
            f"""SELECT explode(sequence(
                to_date('{year}-01-01'), to_date('{year}-12-01'), interval 1 month
            )) as month"""
        )

        # Cross join accounts with months to get full grid
        full_grid = accounts.crossJoin(months_df)

        # Join with ledger data
        with_ledger = full_grid.join(
            ledger_monthly,
            ["account_id", "year", "month"],
            "left"
        )

        # Join with estimates - drop month from estimates to avoid ambiguity
        with_estimates = with_ledger.join(
            silver_estimates.drop("month"),
            ["account_id", "year", "currency"],
            "left"
        )

        # Calculate settled actuals per account (sum up to holdback)
        settled_actuals = (
            ledger_monthly.filter(F.col("month") <= holdback_date)
            .groupBy("account_id", "year")
            .agg(F.sum("ledger_amount").alias("total_settled"))
        )

        # Join settled actuals back
        with_settled = with_estimates.join(
            settled_actuals,
            ["account_id", "year"],
            "left"
        )

        # Calculate remaining months
        holdback_month = holdback_date.month
        remaining_months = 12 - holdback_month

        # Calculate unified amount with source quality
        def calc_amount():
            # Settled period: use ledger amount if present, otherwise fallback
            settled = F.when(
                (F.col("month") <= holdback_date) & (F.col("ledger_amount").isNotNull()),
                F.col("ledger_amount")
            )

            # Fallback for missing ledger before holdback
            settled_fallback = F.when(
                (F.col("month") <= holdback_date) & (F.col("ledger_amount").isNull()),
                F.lit(0.0)
            )

            # Projected period
            # Monthly mode: use estimate directly
            monthly_proj = F.when(
                (F.col("month") > holdback_date) & (F.col("estimation_mode") == "M"),
                F.col("estimate")
            )

            # Annual mode: linear distribution
            annual_proj = F.when(
                (F.col("month") > holdback_date) & (F.col("estimation_mode") == "A"),
                F.greatest(
                    F.lit(0),
                    (F.col("estimate") - F.col("total_settled")) / F.lit(remaining_months)
                )
            )

            return settled.otherwise(
                settled_fallback.otherwise(
                    monthly_proj.otherwise(annual_proj)
                )
            )

        # Determine source
        def calc_source():
            return F.when(
                F.col("month") <= holdback_date,
                F.lit("ledger")
            ).otherwise(F.lit("estimate"))

        # Determine source quality
        def calc_source_quality():
            return F.when(
                (F.col("month") <= holdback_date) & (F.col("ledger_amount").isNull()),
                F.lit("ESTIMATE_FALLBACK")
            ).otherwise(F.lit("CONFIRMED"))

        # Determine if settled
        def calc_is_settled():
            return F.col("month") <= holdback_date

        result = (
            with_settled
            .withColumn("amount", calc_amount())
            .withColumn("source", calc_source())
            .withColumn("source_quality", calc_source_quality())
            .withColumn("is_settled", calc_is_settled())
            .withColumn("holdback_date", F.lit(holdback_date))
            .withColumn("last_calculated_at", F.current_timestamp())
            .withColumn("model_version", F.lit("2"))
            .select(
                "account_id",
                "year",
                "month",
                "holdback_date",
                "amount",
                "is_settled",
                "source",
                "source_quality",
                "last_calculated_at",
                "model_version",
            )
        )

        return result

    def get_holdback_dates() -> DataFrame:
        """Read holdback dates from control table or use defaults."""
        try:
            return spark.read.format("delta").load(control_path)
        except Exception:
            # Return empty DataFrame with correct schema if table doesn't exist
            return spark.createDataFrame([], control_schema)

    def propagate_to_gold(batch_df, batch_id):
        """ForeachBatch callback: recalculate Gold for changed contexts."""
        # Extract changed business context keys from CDF batch
        changed_keys = batch_df.select("account_id", "year").distinct().collect()

        # Get holdback dates from control table
        holdback_df = get_holdback_dates()

        # Process each changed account independently
        for row in changed_keys:
            account_id = row.account_id
            year = row.year

            # Look up holdback date for this account
            account_holdback = holdback_df.filter(
                (F.col("account_id") == account_id) & (F.col("year") == year)
            ).collect()

            if account_holdback:
                holdback_date = account_holdback[0].holdback_date
            else:
                # Default holdback date if not in control table
                holdback_date = dt.date(2026, 3, 31)

            # Calculate unified cashflow for this account
            unified_df = calculate_unified_cashflow(account_id, year, holdback_date)

            # Use MERGE for atomic upsert instead of delete + append
            delta_table = DeltaTable.forPath(spark, gold_path)

            (
                delta_table.alias("t")
                .merge(
                    unified_df.alias("s"),
                    """t.account_id = s.account_id
                       AND t.year = s.year
                       AND t.month = s.month
                       AND t.holdback_date = s.holdback_date""",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

    def start_gold_pipeline():
        """Run Structured Streaming in batch mode (availableNow=True)."""
        checkpoint_dir = "s3a://lakehouse/delta/checkpoints/gold_unified"

        # For MVP, we trigger on estimate changes
        query = (
            spark.readStream.format("delta")
            .option("readChangeFeed", "true")
            .load(silver_estimate_path)
            .writeStream.foreachBatch(propagate_to_gold)
            .option("checkpointLocation", checkpoint_dir)
            .trigger(availableNow=True)
            .start()
        )

        query.awaitTermination()
        return query

    return (
        calculate_unified_cashflow,
        gold_path,
        init_control,
        init_gold,
        start_gold_pipeline,
    )


@app.cell
def _(init_control, init_gold, start_gold_pipeline):
    init_control()
    init_gold()
    query = start_gold_pipeline()
    return


@app.cell
def _(gold_path, spark):
    gold_df = spark.read.format("delta").load(gold_path).orderBy("account_id", "month")
    gold_df.show(truncate=False)
    return


@app.cell
def _(F, calculate_unified_cashflow, dt):
    """Test 1: Accurate Totals for Annual Mode"""
    # For Account 1: Annual target = 12000, Settled = 2400, Remaining = 9600
    # For 9 months: 9600/9 = 1066.67 per month
    _test_holdback = dt.date(2026, 3, 31)
    _test_df = calculate_unified_cashflow("0001", 2026, _test_holdback)

    # Filter for Account 1
    _account_1 = _test_df.filter(F.col("account_id") == "0001")

    # Calculate total
    total = _account_1.agg(F.sum("amount").alias("total")).collect()[0].total

    # Should equal the annual target (12000) minus any rounding
    # Allow small tolerance for floating point
    assert abs(total - 12000.0) < 0.01, f"Expected 12000.0, got {total}"

    print(f"✓ Test 1 Passed: Annual total = {total} (expected ~12000)")
    return


@app.cell
def _(F, calculate_unified_cashflow, dt):
    """Test 2: Idempotency - Re-running calculation produces same results"""
    _test_holdback = dt.date(2026, 3, 31)

    # Run calculation twice for same account
    _df1 = calculate_unified_cashflow("0001", 2026, _test_holdback)
    _df2 = calculate_unified_cashflow("0001", 2026, _test_holdback)

    # Compare row counts
    count1 = _df1.count()
    count2 = _df2.count()
    assert count1 == count2, f"Row count mismatch: {count1} vs {count2}"

    # Compare sums
    sum1 = _df1.agg(F.sum("amount").alias("s")).collect()[0].s
    sum2 = _df2.agg(F.sum("amount").alias("s")).collect()[0].s
    assert abs(sum1 - sum2) < 0.01, f"Sum mismatch: {sum1} vs {sum2}"

    print(f"✓ Test 2 Passed: Idempotency verified ({count1} rows, sum={sum1})")
    return


@app.cell
def _(F, calculate_unified_cashflow, dt):
    """Test 3: Bitemporal Support - Different holdback dates produce different results"""
    # March holdback
    march_df = calculate_unified_cashflow("0001", 2026, dt.date(2026, 3, 31))

    # June holdback
    june_df = calculate_unified_cashflow("0001", 2026, dt.date(2026, 6, 30))

    # Get Account 1 projections for April (month 4)
    april_march = march_df.filter(
        (F.col("account_id") == "0001") & (F.col("month") == dt.date(2026, 4, 1))
    ).collect()

    april_june = june_df.filter(
        (F.col("account_id") == "0001") & (F.col("month") == dt.date(2026, 4, 1))
    ).collect()

    # In March holdback, April is projected
    assert len(april_march) == 1, "April should exist in March holdback"
    assert april_march[0].is_settled == False, "April should be projected in March holdback"

    # In June holdback, April is settled
    assert len(april_june) == 1, "April should exist in June holdback"
    assert april_june[0].is_settled == True, "April should be settled in June holdback"

    print("✓ Test 3 Passed: Bitemporal support verified")
    print(f"  March holdback: April amount = {april_march[0].amount}, settled = {april_march[0].is_settled}")
    print(f"  June holdback: April amount = {april_june[0].amount}, settled = {april_june[0].is_settled}")
    return


@app.cell
def _():
    """Test 4: Monthly Mode - Direct monthly estimates used"""
    # First, create a monthly estimate batch
    # This will be tested after we add monthly estimates
    print("✓ Test 4: Monthly mode test (run after adding monthly estimates)")
    return


@app.cell
def _(dt, make_estimate_batch):
    # Batch 2: change account 0001 from annual to monthly (grain switch A -> M)
    # Keep account 0002 unchanged (but we don't include it in this batch)
    # Add a new account 0004 as annual.
    batch_2 = make_estimate_batch(
        [
            ("M", str(1).zfill(4), 2026, dt.date(2026, 1, 1), "USD", 3000.0),
            ("M", str(1).zfill(4), 2026, dt.date(2026, 2, 1), "USD", 3200.0),
            ("M", str(1).zfill(4), 2026, dt.date(2026, 3, 1), "USD", 3100.0),
            ("A", str(4).zfill(4), 2026, None, "USD", 15000.0),
        ],
    )
    return (batch_2,)


@app.cell
def _(append_bronze_estimates, batch_2, merge_estimates_to_silver):
    append_bronze_estimates(batch_2)
    merge_estimates_to_silver(batch_2)
    return


@app.cell
def _(F, calculate_unified_cashflow, dt):
    """Test 4 (continued): Monthly Mode - Direct monthly estimates used"""
    _test_holdback = dt.date(2026, 3, 31)
    _test_df = calculate_unified_cashflow("0001", 2026, _test_holdback)

    # For Account 1 in Monthly mode:
    # - Jan-Mar: Ledger actuals (800 each)
    # - Apr-Dec: Monthly estimates (1300, 1400, 1500, ...)

    _account_1 = _test_df.filter(F.col("account_id") == "0001").orderBy("month")

    # Check January (settled)
    jan = _account_1.filter(F.col("month") == dt.date(2026, 1, 1)).collect()[0]
    assert jan.is_settled == True, "January should be settled"
    assert jan.source == "ledger", "January source should be ledger"
    assert jan.amount == 800.0, f"January amount should be 800, got {jan.amount}"

    # Check April (projected)
    apr = _account_1.filter(F.col("month") == dt.date(2026, 4, 1)).collect()[0]
    assert apr.is_settled == False, "April should be projected"
    assert apr.source == "estimate", "April source should be estimate"
    assert apr.amount == 3100.0, f"April amount should be 3100, got {apr.amount}"

    print("✓ Test 4 Passed: Monthly mode uses direct estimates")
    print(f"  January: {jan.amount} (settled={jan.is_settled})")
    print(f"  April: {apr.amount} (settled={apr.is_settled})")
    return


@app.cell
def _(bronze_estimates, conn, mo):
    _df = mo.sql(
        f"""
        select * from bronze_estimates
        """,
        engine=conn
    )
    return


@app.cell
def _(conn, mo, silver_estimates):
    _df = mo.sql(
        f"""
        select * from silver_estimates;
        """,
        engine=conn
    )
    return


@app.cell
def _(dt, make_estimate_batch):
    batch_3 = make_estimate_batch(
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
def _(append_bronze_estimates, batch_3, merge_estimates_to_silver):
    append_bronze_estimates(batch_3)
    merge_estimates_to_silver(batch_3)
    return


@app.cell
def _(bronze_estimates, conn, mo):
    _df = mo.sql(
        f"""
        select * from bronze_estimates
        """,
        engine=conn
    )
    return


@app.cell
def _(conn, mo, silver_estimates):
    _df = mo.sql(
        f"""
        SELECT * from silver_estimates
        """,
        engine=conn
    )
    return


@app.cell
def _(
    append_bronze_estimates,
    dt,
    make_estimate_batch,
    merge_estimates_to_silver,
):
    # add new estimates monthly for the given context
    batch_m = make_estimate_batch(
        [
            ("M", str(99).zfill(4), 2026, dt.date(2026, 1, 1), "USD", 9999.0),
            ("M", str(99).zfill(4), 2026, dt.date(2026, 4, 1), "USD", 9999.0),
        ]
    )

    # test M -> A change
    batch_a = make_estimate_batch(
        [
            ("A", str(99).zfill(4), 2026, None, "USD", 1000.0),
        ]
    )

    append_bronze_estimates(batch_m)
    merge_estimates_to_silver(batch_m)

    append_bronze_estimates(batch_a)
    merge_estimates_to_silver(batch_a)
    return


@app.cell
def _(bronze_estimates, conn, mo):
    _df = mo.sql(
        f"""
        select * from bronze_estimates
        """,
        engine=conn
    )
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
def _(silver_estimate_path, spark):
    # Demonstrate downstream consumption of the Silver CDF.
    # This shows every row that changed in Silver across all versions.
    # Use startingTimestamp to avoid retention issues - use a time well before table creation
    from datetime import datetime, timedelta
    start_time = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    cdf_df = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingTimestamp", start_time)
        .load(silver_estimate_path)
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
def _():
    return


if __name__ == "__main__":
    app.run()
