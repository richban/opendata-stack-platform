import marimo

__generated_with = "0.21.1"
app = marimo.App(width="full")


@app.cell
def _():
    import datetime as dt
    import random
    import sys

    from pathlib import Path

    import pyspark.sql.types as T

    sys.path.insert(0, str(Path(__file__).parent))

    from config import create_delta_spark_session, get_s3_store

    return T, create_delta_spark_session, dt, get_s3_store, random


@app.cell
def _(create_delta_spark_session, get_s3_store):
    store = get_s3_store()
    spark, conn = create_delta_spark_session("s3a://lakehouse/delta")
    return conn, spark


@app.cell
def _(T):
    schema = T.StructType(
        [
            T.StructField("estimation_mode", T.StringType(), False), # annual or Monthly
            T.StructField("account_id", T.StringType(), False),  # natural key
            T.StructField("year", T.IntegerType(), False),  # year in question
            T.StructField("month", T.DateType(), True),  # only null if annual estimation mode
            T.StructField("currency", T.StringType(), False),
            T.StructField("estimate", T.DoubleType(), False),
            T.StructField("inserted_at", T.TimestampType(), False)
        ]
    )
    return


@app.cell
def _(dt, random, spark):
    cols = [
        "estimation_mode",
        "account_id",
        "year",
        "month",
        "currency",
        "estimate",
        "batch_id"
    ]

    batch_id = str(random.randint(0, 999)).zfill(4)

    df = spark.createDataFrame([
        ("A", str(1).zfill(4), 2026, None, "USD", 10000.0, batch_id),
        ("A", str(2).zfill(4), 2026, None, "USD", 10000.0, batch_id),
        ("M", str(3).zfill(4), 2026, dt.date(2026, 1, 1), "USD", 10000.0, batch_id),
        ("M", str(3).zfill(4), 2026, dt.date(2026, 2, 1), "USD", 10000.0, batch_id),
        ("M", str(3).zfill(4), 2026, dt.date(2026, 3, 1), "USD", 10000.0, batch_id),
    ], cols)
    return (df,)


@app.cell
def _(df):
    df
    return


@app.cell
def _(df):
    def append_bronze(df):
        df.write.format('delta').mode('append').saveAsTable('bronze')

    append_bronze(df)
    return


@app.cell
def _():
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(bronze, conn, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM bronze
        """,
        engine=conn
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
