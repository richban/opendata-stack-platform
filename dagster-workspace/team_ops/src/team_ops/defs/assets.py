"""Dagster assets for Streamify using Spark Connect.

Assets use direct PySpark API via SparkSession resource injection.
"""

from datetime import date, timedelta

import dagster as dg
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

from team_ops.defs.resources import SparkConnectResource


@dg.asset(
    group_name="silver",
    compute_kind="spark",
    description="Deduplicated listen events from Bronze",
)
def silver_listen_events(context: dg.AssetExecutionContext, spark: SparkConnectResource):
    """Deduplicate listen_events from Bronze to Silver."""
    event_date = (date.today() - timedelta(days=1)).isoformat()
    context.log.info(f"Processing listen_events for {event_date}")

    bronze_table = "lakehouse.streamify.bronze_listen_events"
    silver_table = "lakehouse.streamify.silver_listen_events"

    session = spark.get_session()

    bronze_df = session.table(bronze_table).filter(col("event_date") == event_date)
    input_count = bronze_df.count()
    context.log.info(f"Input: {input_count} records")

    if input_count == 0:
        return

    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {silver_table}
        USING iceberg
        PARTITIONED BY (event_date)
        AS SELECT * FROM {bronze_table} WHERE 1=0
    """)

    window_spec = Window.partitionBy("event_id").orderBy("_processing_time")
    deduped_df = (
        bronze_df.withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    output_count = deduped_df.count()

    session.sql(f"DELETE FROM {silver_table} WHERE event_date = '{event_date}'")
    deduped_df.writeTo(silver_table).append()

    context.log.info(f"Output: {output_count} records")
    context.add_output_metadata(
        {
            "input_count": input_count,
            "output_count": output_count,
            "duplicates_removed": input_count - output_count,
        }
    )
