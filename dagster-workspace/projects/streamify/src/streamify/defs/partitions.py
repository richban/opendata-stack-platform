"""Partition definitions for Streamify data assets.

This module contains all partition definitions used across Bronze, Silver, and Gold
layers. Centralizing partitions ensures consistency and makes it easy to modify
date ranges or partition strategies.

By default, partitions are open-ended (no end_date), meaning they automatically
extend to the current date and new partitions are added as time progresses.
To close the partition range, set the END_DATE environment variable or modify
the end_date parameter directly.
"""

import os

import dagster as dg

START_DATE = "2026-03-21"
END_DATE = None

# Daily partitions for Bronze layer (event ingestion)
# Open-ended: new daily partitions are automatically available as time progresses
bronze_daily_partition = dg.DailyPartitionsDefinition(
    start_date=START_DATE,
    end_date=END_DATE,
)

# Daily partitions for Silver layer (deduplication and cleaning)
silver_daily_partition = dg.DailyPartitionsDefinition(
    start_date=START_DATE,
    end_date=END_DATE,
)

# Daily partitions for Gold layer (aggregations)
gold_daily_partition = dg.DailyPartitionsDefinition(
    start_date=START_DATE,
    end_date=END_DATE,
)

# Monthly partitions for Gold layer (monthly rollup aggregations)
gold_monthly_partition = dg.MonthlyPartitionsDefinition(
    start_date=START_DATE,
    end_date=END_DATE,
)

# Weekly partitions for Gold layer (weekly rollup aggregations)
gold_weekly_partition = dg.WeeklyPartitionsDefinition(
    start_date=START_DATE,
    end_date=END_DATE,
)
