# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "marimo",
#     "duckdb",
#     "pyiceberg",
#     "pandas",
#     "pyspark",
#     "pyarrow>=15.0.0",
#     "grpcio>=1.48.1",
#     "grpcio-status>=1.48.1",
#     "obstore",
# ]
# ///

import marimo

__generated_with = "0.21.1"
app = marimo.App(
    width="medium",
    app_title="Iceberg Partitioning and Clustering Guide",
)


@app.cell
def _():
    import logging
    import sys
    import datetime
    from pathlib import Path
    import ibis
    import marimo as mo

    sys.path.insert(0, str(Path(__file__).parent))

    from config import (
        create_duckdb_connection,
        create_iceberg_catalog,
        create_spark_session,
        get_minio_config,
        get_polaris_config,
        get_s3_store,
    )

    # Now insert more data - it will use the new partition spec
    from datetime import datetime, timedelta
    import random

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    polaris_config = get_polaris_config()
    minio_config = get_minio_config()
    return (
        create_duckdb_connection,
        create_iceberg_catalog,
        create_spark_session,
        datetime,
        get_s3_store,
        mo,
        random,
        timedelta,
    )


@app.cell
def _(create_iceberg_catalog):
    catalog = create_iceberg_catalog()
    return (catalog,)


@app.cell
def _(catalog):
    existing_namespaces = catalog.list_namespaces()
    if ("iceberg_study",) not in existing_namespaces:
        catalog.create_namespace(
            "iceberg_study",
            properties={"location": "s3://lakehouse/iceberg_study/"},
        )
        print("✓ Created namespace: iceberg_study")
    else:
        print("✓ Namespace already exists: iceberg_study")
    return


@app.cell
def _(create_duckdb_connection):
    con = create_duckdb_connection()
    return (con,)


@app.cell
def _(create_spark_session):
    spark, spark_conn = create_spark_session()
    return spark, spark_conn


@app.cell
def _(get_s3_store):
    store = get_s3_store()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # Apache Iceberg Partitioning and Clustering Deep Dive

    **Understanding partitions, their evolution, and how clustering improves query performance**

    This notebook explores Iceberg's powerful partitioning system, hidden partitioning, partition evolution, and clustering techniques.

    ## What You'll Learn

    1. **Hidden Partitioning** - How Iceberg decouples physical from logical partitioning
    2. **Partition Transforms** - Identity, bucket, truncate, year, month, day, hour transforms
    3. **Partition Evolution** - Changing partitioning schemes without rewriting data
    4. **Partition Pruning** - How queries skip irrelevant data files
    5. **Clustering and Z-Ordering** - Data layout strategies for performance
    6. **Performance Trade-offs** - When partitioning helps and when it hurts
    7. **Limitations and Best Practices** - What works and what doesn't
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 1: What "Hidden Partitioning" Actually Means

    ### The Physical Reality

    **Both Hive and Iceberg create partition directories:**

    ```
    Physical Layout (Hive AND Iceberg):
    ├── year=2023/
    │   ├── month=01/
    │   │   └── data.parquet
    │   └── month=02/
    │       └── data.parquet
    └── year=2024/
        └── month=01/
            └── data.parquet

    OR with bucket transforms (Iceberg only):
    ├── user_id_bucket=5/
    │   └── event_time_year=2023/
    │       └── 00001-abc.parquet
    ```

    **The files are stored in partition directories in both systems.**

    ### The Real Difference: User Experience

    **What "Hidden" Actually Means:**

    | Aspect | Hive | Iceberg |
    |--------|------|---------|
    | **Table Schema** | Must include partition columns | Partition columns NOT in schema |
    | **INSERT** | `INSERT ... PARTITION (year=2023)` | Just `INSERT` - partitioning is automatic |
    | **Query** | Must filter on partition columns | Filter on source columns, Iceberg maps to partitions |
    | **Transforms** | None - raw values only | bucket(), truncate(), years(), months(), days(), hours() |
    | **Evolution** | Impossible without rewrite | Add partitions without rewriting data |

    **Example:**

    ```sql
    -- Hive: You must manage partitions manually
    CREATE TABLE hive_events (
        year INT,
        month INT,
        user_id BIGINT,
        event_type STRING
    ) PARTITIONED BY (year, month);

    INSERT INTO hive_events
    VALUES (2023, 6, 42, 'click');  -- You must compute and specify year/month

    -- Query must use partition columns
    SELECT * FROM hive_events WHERE year = 2023 AND month = 6;

    -- Iceberg: Partitioning is automatic and transparent
    CREATE TABLE iceberg_events (
        event_time TIMESTAMP,
        user_id BIGINT,
        event_type STRING
    ) PARTITIONED BY (years(event_time));  -- No year column needed!

    INSERT INTO iceberg_events
    VALUES (TIMESTAMP '2023-06-15 10:30:00', 42, 'click');  -- Just the data

    -- Query uses source column, Iceberg handles partitioning
    SELECT * FROM iceberg_events WHERE event_time >= '2023-06-01';
    ```

    ### So What Makes It "Hidden"?

    1. **Hidden from schema** - Partition columns don't pollute your table definition
    2. **Hidden from inserts** - You insert data, Iceberg computes partition values
    3. **Hidden from queries** - You filter on natural columns, Iceberg maps to partitions
    4. **Hidden complexity** - Transforms happen automatically

    **The directories are still there - you just don't have to think about them!**
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Demonstration: Creating a Partitioned Table

    Let's create a table with hidden partitioning to see how it works:
    """)
    return


@app.cell
def _(datetime, mo, random, spark, timedelta):
    # Set the current catalog to lakehouse
    spark.catalog.setCurrentCatalog("lakehouse")

    # Create namespace if not exists
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_study")

    # Drop if exists and create new partitioned table
    spark.sql("DROP TABLE IF EXISTS iceberg_study.events_partitioned")

    spark.sql(
        """
        CREATE TABLE iceberg_study.events_partitioned (
            event_time TIMESTAMP,
            user_id LONG,
            event_type STRING,
            amount DOUBLE
        ) USING ICEBERG
        PARTITIONED BY (bucket(16, user_id), years(event_time))
    """
    )

    _data = []
    _base_time = datetime(2023, 1, 1)

    for _i in range(1000):
        # Spread data across 2 years
        _days_offset = random.randint(0, 730)
        _event_time = _base_time + timedelta(days=_days_offset)
        _user_id = random.randint(1, 1000)
        _event_type = random.choice(["click", "purchase", "view", "logout"])
        _amount = round(random.uniform(10, 500), 2)
        _data.append((_event_time, _user_id, _event_type, _amount))

    _df = spark.createDataFrame(_data, ["event_time", "user_id", "event_type", "amount"])
    _df.writeTo("iceberg_study.events_partitioned").append()

    mo.md(
        """
    **Created partitioned table:**
    - Table: `iceberg_study.events_partitioned`
    - Partitioned by: `bucket(16, user_id)` and `years(event_time)`
    - Rows: 1,000
    - Partition columns are **HIDDEN** - not in the table schema!
    """
    )
    return


@app.cell
def _(mo, spark_conn):
    _df = mo.sql(
        f"""
        SELECT *
        FROM lakehouse.iceberg_study.events_partitioned
        LIMIT 10
        """,
        engine=spark_conn
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    **Notice:** The query returns data without any partition columns visible. The partition values are stored in metadata only!

    Let's look at the partition information in the metadata:
    """)
    return


@app.cell
def _(mo, spark_conn):
    _df = mo.sql(
        f"""
        SELECT 
            partition,
            record_count,
            file_count
        FROM lakehouse.iceberg_study.events_partitioned.partitions
        LIMIT 20
        """,
        engine=spark_conn
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT 
            manifest_path,
            added_files_count,
            added_rows_count,
            *
        FROM read_avro('s3://lakehouse/iceberg_study/events_partitioned/metadata/snap-1843138130664075904-1-9877a105-4c71-4794-863d-556cf7780323.avro');
        """,
        engine=con
    )
    return


@app.cell(hide_code=True)
def _(con, mo):
    _df = mo.sql(
        f"""
        select * from read_avro("s3://lakehouse/iceberg_study/events_partitioned/metadata/9877a105-4c71-4794-863d-556cf7780323-m0.avro")
        """,
        engine=con
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 2: Partition Transforms Explained

    Iceberg provides powerful partition transforms that derive partition values from source columns:

    ### Transform Types

    | Transform | Syntax | Description | Use Case |
    |-----------|--------|-------------|----------|
    | **Identity** | `column` | Use column value directly | Low cardinality enums, categories |
    | **Bucket** | `bucket(N, column)` | Hash into N buckets | High cardinality columns (user_id) |
    | **Truncate** | `truncate(L, column)` | Truncate to L chars/bytes | Strings, timestamps by minute |
    | **Year** | `years(column)` | Extract year from timestamp | Time-series by year |
    | **Month** | `months(column)` | Extract year-month | Time-series by month |
    | **Day** | `days(column)` | Extract date | Time-series by day |
    | **Hour** | `hours(column)` | Extract hour | Time-series by hour |

    ### Bucket Transform Deep Dive

    Bucketing distributes data evenly across a fixed number of buckets:

    ```
    Input: user_id values [1, 2, 3, 4, 5, ... 1000000]

    bucket(16, user_id):
    ┌─────────────────────────────────────────────────────────────┐
    │ Bucket 0:  user_id % 16 == 0   (IDs: 16, 32, 48, ...)      │
    │ Bucket 1:  user_id % 16 == 1   (IDs: 1, 17, 33, ...)       │
    │ Bucket 2:  user_id % 16 == 2   (IDs: 2, 18, 34, ...)       │
    │ ...                                                         │
    │ Bucket 15: user_id % 16 == 15  (IDs: 15, 31, 47, ...)      │
    └─────────────────────────────────────────────────────────────┘

    Benefits:
    - Even distribution (no skew)
    - Join optimization (co-located data)
    - Parallelism control (exactly N buckets)
    ```
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Time-Based Transforms

    Time transforms extract date/time components for partitioning:

    ```
    Input: event_time = '2023-07-15 14:30:00'

    Transform Results:
    ┌─────────────────────────────────────────────────────────────┐
    │ years(event_time)  → 2023                                   │
    │ months(event_time) → 2023-07 (stored as months since 1970-01 = 642)│
    │ days(event_time)   → 2023-07-15 (stored as days since 1970-01-01) │
    │ hours(event_time)  → 2023-07-15 14:00 (stored as hours since 1970-01-01 00:00)│
    └─────────────────────────────────────────────────────────────┘
    ```

    **Important:** Time transforms use normalized integer representations internally for efficiency:
    - `years`: Years since 1970
    - `months`: Months since 1970-01
    - `days`: Days since 1970-01-01
    - `hours`: Hours since 1970-01-01 00:00

    This allows efficient range comparisons and evolution between time granularities!
    """)
    return


@app.cell
def _(datetime, mo, spark):
    # Create a table demonstrating all transform types
    spark.catalog.setCurrentCatalog("lakehouse")
    spark.sql("DROP TABLE IF EXISTS iceberg_study.transform_demo")

    spark.sql(
        """
        CREATE TABLE iceberg_study.transform_demo (
            id LONG,
            category STRING,
            description STRING,
            event_time TIMESTAMP,
            amount DECIMAL(10,2)
        ) USING ICEBERG
        PARTITIONED BY (
            category,                           -- Identity transform
            bucket(8, id),                      -- Bucket transform
            truncate(10, description),          -- Truncate transform
            months(event_time)                  -- Time transform
        )
    """
    )

    # Insert sample data
    data = [
        (
            1,
            "electronics",
            "Smartphone model XYZ2023ABC",
            datetime(2023, 1, 15, 10, 30),
            999.99,
        ),
        (
            2,
            "electronics",
            "Laptop model ABC",
            datetime(2023, 1, 20, 14, 0),
            1299.99,
        ),
        (3, "clothing", "T-shirt cotton", datetime(2023, 2, 1, 9, 0), 29.99),
        (
            4,
            "clothing",
            "Jeans denim blue",
            datetime(2023, 2, 15, 16, 30),
            79.99,
        ),
        (5, "food", "Organic apples 1kg", datetime(2023, 3, 10, 11, 0), 4.99),
        (6, "food", "Fresh bread sourdough", datetime(2023, 3, 20, 8, 0), 5.49),
    ]

    _df = spark.createDataFrame(
        data, ["id", "category", "description", "event_time", "amount"]
    )
    _df.writeTo("iceberg_study.transform_demo").append()

    mo.md(
        """
    **Created table with multiple transform types:**
    - `category` - Identity (use value as-is)
    - `bucket(8, id)` - Hash id into 8 buckets
    - `truncate(10, description)` - First 10 characters of description
    - `months(event_time)` - Year-month extraction
    """
    )
    _df
    return


@app.cell
def _(mo, spark):
    # Query partition metadata using Spark SQL directly
    _df = spark.sql(
        "SELECT * FROM iceberg_study.transform_demo.partitions ORDER BY partition"
    ).toPandas()
    mo.output.replace(_df)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    **Observe:**
    1. Each partition shows the actual partition values (not the source values)
    2. `bucket(8, id)` shows bucket numbers (0-7)
    3. `truncate(10, description)` shows first 10 chars
    4. `months(event_time)` shows months since 1970-01 (e.g., 637 = 2023-02)
    5. Identity transform shows the actual category values

    This is **hidden partitioning** - partition values are derived, not stored in the data!
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 3: Partition Evolution

    One of Iceberg's most powerful features is **partition evolution** - the ability to change partitioning schemes without rewriting existing data.

    ### How Partition Evolution Works

    ```
    Timeline with Partition Evolution:

    Snapshot 1:                    Snapshot 2 (after evolution):
    ┌────────────────────┐         ┌──────────────────────────────┐
    │ Partition Spec v1  │         │ Partition Spec v2            │
    │ - days(event_time) │    →    │ - days(event_time)           │
    │                    │         │ - bucket(16, user_id)        │
    └────────────────────┘         └──────────────────────────────┘

    Existing Data:                 New Data:
    ┌────────────────────┐         ┌──────────────────────────────┐
    │ File A (2023-01-01)│         │ File B (2023-01-02, bucket3) │
    │ File B (2023-01-02)│         │ File C (2023-01-03, bucket7) │
    └────────────────────┘         └──────────────────────────────┘

    Queries work across ALL snapshots with appropriate filters!
    ```

    ### Rules for Partition Evolution

    ✅ **Possible:**
    - Add new partition fields
    - Evolve time granularity (day → hour, month → day)
    - Add bucketing to existing partition

    ❌ **Not Possible:**
    - Remove partition fields (would break old queries)
    - Change bucket count
    - Change truncate length
    - Make partitions more coarse (day → month)

    **Key insight:** Iceberg maintains partition spec history. Each snapshot references the spec used to write it!
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Demonstration: Evolving Partitions

    Let's evolve the partitioning scheme of our events table:
    """)
    return


@app.cell
def _(mo, spark):
    # Inspect partitions using Spark SQL directly
    _df = spark.sql(
        "SELECT * FROM iceberg_study.events_partitioned.partitions LIMIT 5"
    ).toPandas()
    mo.output.replace(_df)
    return


@app.cell
def _(catalog, mo):
    # Evolve partition spec using PyIceberg API (Spark SQL ALTER TABLE has a
    # serialization bug in Spark Master that prevents this from working there)
    from pyiceberg.transforms import IdentityTransform

    _tbl = catalog.load_table("iceberg_study.events_partitioned")
    with _tbl.update_spec() as update:
        update.add_field(
            source_column_name="event_type",
            transform=IdentityTransform(),
            partition_field_name="event_type",
        )
    mo.md("Partition field 'event_type' added successfully via PyIceberg API")
    return


@app.cell
def _(datetime, mo, random, spark, timedelta):
    _data2 = []
    _base_time2 = datetime(2023, 6, 1)

    for _i2 in range(500):
        _days_offset2 = random.randint(0, 180)
        _event_time2 = _base_time2 + timedelta(days=_days_offset2)
        _user_id2 = random.randint(1, 1000)
        _event_type2 = random.choice(["click", "purchase", "view", "logout"])
        _amount2 = round(random.uniform(10, 500), 2)
        _data2.append((_event_time2, _user_id2, _event_type2, _amount2))

    _df2 = spark.createDataFrame(
        _data2, ["event_time", "user_id", "event_type", "amount"]
    )
    _df2.writeTo("iceberg_study.events_partitioned").append()

    mo.md(
        """
    **Partition Evolution Applied:**
    - Added `event_type` as a new partition field
    - Inserted 500 new rows using the updated partition spec
    - Old data remains with original partitioning
    - New data uses the new 3-field partitioning
    """
    )
    return


@app.cell
def _(mo, spark_conn):
    _df = mo.sql(
        f"""
        SELECT * FROM lakehouse.iceberg_study.events_partitioned.partitions
        """,
        engine=spark_conn
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    **Key Observations:**

    1. **Multiple partition specs exist** - spec_id 0 (original) and spec_id 1 (evolved)
    2. **Old partitions lack event_type** - they have NULL for the new field
    3. **New partitions have all 3 fields** - user_id_bucket, event_time_year, event_type
    4. **Queries work seamlessly** - Iceberg handles the different specs transparently

    ### When to Use Partition Evolution

    ✅ **Good use cases:**
    - Adding granularity (e.g., month → day partitioning as data grows)
    - Adding bucketing for join optimization
    - Adding category partitions for better pruning

    ⚠️ **Be cautious:**
    - Too many partition fields create small files
    - Query planning overhead increases with spec history
    - Old data won't benefit from new partitions
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 4: Partition Pruning and Query Optimization

    Partition pruning is the process of skipping irrelevant data files based on query predicates. This is where partitioning delivers performance benefits.

    ### How Partition Pruning Works

    ```
    Query: SELECT * FROM events WHERE user_id = 42 AND event_time >= '2023-06-01'

    Table Partitioned by: bucket(16, user_id), days(event_time)

    Step 1: Compute partition filters from query
    ┌─────────────────────────────────────────────────────────────────┐
    │ user_id = 42 → bucket(16, 42) = 10                              │
    │ event_time >= '2023-06-01' → days >= 19509                      │
    │                                                                 │
    │ Partition filter: (user_id_bucket = 10) AND (days >= 19509)     │
    └─────────────────────────────────────────────────────────────────┘

    Step 2: Scan manifest files
    ┌─────────────────────────────────────────────────────────────────┐
    │ Manifest A: partition={user_id_bucket: 5, days: 19500}          │
    │             → SKIP (bucket 5 ≠ 10)                              │
    │                                                                 │
    │ Manifest B: partition={user_id_bucket: 10, days: 19510}         │
    │             → CHECK (matches both filters)                      │
    │                                                                 │
    │ Manifest C: partition={user_id_bucket: 10, days: 19495}         │
    │             → SKIP (days < 19509)                               │
    └─────────────────────────────────────────────────────────────────┘

    Step 3: Read only matching data files
    Result: Only files from Manifest B are read → Massive I/O reduction!
    ```
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Demonstration: Partition Pruning in Action

    Let's compare query performance with and without partition pruning:
    """)
    return


@app.cell
def _(datetime, mo, random, spark, timedelta):
    # Create two tables: one partitioned, one not
    spark.catalog.setCurrentCatalog("lakehouse")

    # Unpartitioned table
    spark.sql("DROP TABLE IF EXISTS iceberg_study.events_unpartitioned")
    spark.sql(
        """
        CREATE TABLE iceberg_study.events_unpartitioned (
            event_time TIMESTAMP,
            user_id LONG,
            event_type STRING,
            amount DOUBLE
        ) USING ICEBERG
    """
    )

    # Partitioned table (same schema, but partitioned)
    spark.sql("DROP TABLE IF EXISTS iceberg_study.events_pruning_test")
    spark.sql(
        """
        CREATE TABLE iceberg_study.events_pruning_test (
            event_time TIMESTAMP,
            user_id LONG,
            event_type STRING,
            amount DOUBLE
        ) USING ICEBERG
        PARTITIONED BY (bucket(16, user_id), days(event_time))
    """
    )

    # Generate dataset — 2,000 rows is sufficient to demonstrate pruning
    # without overwhelming the Spark Connect container during the shuffle
    # (bucket(16) × days(365) can create thousands of output partitions).
    _data3 = []
    _base_time3 = datetime(2023, 1, 1)

    for _i3 in range(1000):
        _days_offset3 = random.randint(0, 365)
        _event_time3 = _base_time3 + timedelta(days=_days_offset3)
        _user_id3 = random.randint(1, 10000)
        _event_type3 = random.choice(["click", "purchase", "view", "logout"])
        _amount3 = round(random.uniform(10, 500), 2)
        _data3.append((_event_time3, _user_id3, _event_type3, _amount3))

    _df3 = spark.createDataFrame(
        _data3, ["event_time", "user_id", "event_type", "amount"]
    )

    # Add guaranteed rows matching the partition pruning test query
    _data3.extend([
        (datetime(2023, 6, 15, 12, 0), 42, "click", 99.99),
        (datetime(2023, 6, 18, 14, 30), 42, "view", 15.50),
        (datetime(2023, 6, 25, 9, 15), 42, "purchase", 250.00)
    ])

    # Write to unpartitioned table first (no shuffle needed)
    _df3.writeTo("iceberg_study.events_unpartitioned").append()

    # Coalesce to a single partition, then sort the data by the partition keys.
    # Because the data is already sorted, Iceberg's Spark extension will NOT
    # inject the cluster-crashing `Exchange` shuffle. The writer will only need
    # to keep one file buffer open at a time.
    _df3.coalesce(1).sortWithinPartitions(
        "user_id", "event_time"
    ).writeTo("iceberg_study.events_pruning_test").append()

    mo.md(
        """
    **Created test tables:**
    - `events_unpartitioned` - No partitioning (2,000 rows)
    - `events_pruning_test` - Partitioned by bucket(16, user_id) + days(event_time) (2,000 rows)

    Both tables contain identical data, but different physical organization.
    """
    )
    return


@app.cell
def _(mo, spark_conn):
    # Query with specific user_id and date range
    # This should benefit from partition pruning
    _df = mo.sql(
        f"""
        SELECT 
            COUNT(*) as row_count,
            AVG(amount) as avg_amount
        FROM lakehouse.iceberg_study.events_pruning_test
        WHERE user_id = 42
          AND event_time >= '2023-06-01'
          AND event_time < '2023-07-01'
        """,
        engine=spark_conn
    )
    return


@app.cell
def _(con, mo):
    # Same query on unpartitioned table
    _df = mo.sql(
        f"""
        SELECT 
            COUNT(*) as row_count,
            AVG(amount) as avg_amount
        FROM lakehouse.iceberg_study.events_unpartitioned
        WHERE user_id = 42
          AND event_time >= '2023-06-01'
          AND event_time < '2023-07-01'
        """,
        engine=con
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Analyzing Partition Pruning Effectiveness

    Let's examine how many files were scanned in each case:
    """)
    return


@app.cell
def _(mo, spark_conn):
    # Get file counts for partitioned table (should be much fewer due to pruning)
    _df = mo.sql(
        f"""
        SELECT 
            COUNT(*) as file_count,
            SUM(record_count) as total_records
        FROM lakehouse.iceberg_study.events_pruning_test.files
        """,
        engine=spark_conn
    )
    return


@app.cell
def _(mo, spark_conn):
    # Show partition distribution
    _df = mo.sql(
        f"""
        SELECT 
            partition,
            COUNT(*) as file_count,
            SUM(record_count) as record_count
        FROM lakehouse.iceberg_study.events_pruning_test.files
        GROUP BY partition
        ORDER BY record_count DESC
        LIMIT 20
        """,
        engine=spark_conn
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Understanding the Pruning Impact

    **Without partitioning:**
    - Query must scan ALL data files (full table scan)
    - 10,000 rows × number of files
    - Predicate applied after reading data

    **With partitioning (user_id bucket + day):**
    - Query scans only files where:
      - `bucket(16, user_id) = bucket(16, 42)` (1/16 of data)
      - `days(event_time)` in June 2023 (~1/12 of data)
    - Combined: ~1/192 of data scanned (roughly 0.5%)

    **Performance improvement:** 100-200× faster for selective queries!

    ### When Partition Pruning Works

    ✅ **Effective predicates:**
    - Equality: `user_id = 42`
    - Range: `event_time >= '2023-06-01'`
    - IN clause: `user_id IN (1, 2, 3)`
    - Combined with AND: `user_id = 42 AND event_time > '2023-01-01'`

    ❌ **Ineffective predicates:**
    - OR conditions: `user_id = 42 OR user_id = 100`
    - Functions on partition column: `DAY(event_time) = 1`
    - Wildcards: `user_id LIKE '42%'`
    - Not all transforms support all predicate types
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 5: Clustering and Z-Ordering

    While partitioning organizes data at the table level, **clustering** (also called Z-Ordering) optimizes the physical layout within files.

    ### The Problem: Data Skew and Locality

    ```
    Poorly Clustered Data:           Well Clustered Data (Z-Ordered):
    ┌──────────────────────┐         ┌──────────────────────┐
    │ File 1: user_id 1-10 │         │ File 1: user_id 1-2  │
    │ File 2: user_id 1-10 │         │ File 2: user_id 1-2  │
    │ File 3: user_id 1-10 │         │ File 3: user_id 3-4  │
    │ File 4: user_id 1-10 │         │ File 4: user_id 3-4  │
    └──────────────────────┘         └──────────────────────┘

    Query: WHERE user_id = 5         Query: WHERE user_id = 5
    Result: Read ALL files           Result: Read 2 files only!
    ```

    ### What is Z-Ordering?

    Z-Ordering is a multi-dimensional clustering technique that co-locates related data:

    ```
    Z-Order Curve (2D example):

    Z-Value: 0  1  2  3  4  5  6  7
    ┌────┬────┬────┬────┬────┬────┬────┬────┐
    │ 00 │ 01 │ 10 │ 11 │ 20 │ 21 │ 30 │ 31 │
    ├────┼────┼────┼────┼────┼────┼────┼────┤
    │ 02 │ 03 │ 12 │ 13 │ 22 │ 23 │ 32 │ 33 │
    ├────┼────┼────┼────┼────┼────┼────┼────┤
    │ 40 │ 41 │ 50 │ 51 │ 60 │ 61 │ 70 │ 71 │
    ├────┼────┼────┼────┼────┼────┼────┼────┤
    │ 42 │ 43 │ 52 │ 53 │ 62 │ 63 │ 72 │ 73 │
    └────┴────┴────┴────┴────┴────┴────┴────┘

    Data points close in Z-order are close in ALL dimensions!
    (user_id, event_time) pairs with similar Z-values are stored together
    ```
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Iceberg's Approach to Clustering

    Iceberg itself doesn't automatically Z-order data, but it provides the foundation:

    1. **SortOrder specification** - Define how data should be sorted on write
    2. **Compaction with sorting** - Rewrite files with optimal clustering
    3. **Statistics** - Min/max values in metadata enable pruning at file level

    ### Sort Order vs Partitioning

    | Aspect | Partitioning | Sorting/Clustering |
    |--------|--------------|-------------------|
    | **Level** | Table-level groups | File-level organization |
    | **Purpose** | Eliminate files | Minimize data within files |
    | **Storage** | Separate directories/files | Within file (row groups) |
    | **Metadata** | Partition values in manifests | Column statistics in footers |
    | **Queries** | Skip entire partitions | Skip row groups within files |

    **Best practice:** Use BOTH! Partition to skip files, cluster to optimize within files.
    """)
    return


@app.cell
def _(datetime, mo, random, spark, timedelta):
    # Demonstrate clustering with Spark's OPTIMIZE (Z-Ordering)
    spark.catalog.setCurrentCatalog("lakehouse")

    # Create a table with many small files (simulating poor clustering)
    spark.sql("DROP TABLE IF EXISTS iceberg_study.clustering_demo")
    spark.sql(
        """
        CREATE TABLE iceberg_study.clustering_demo (
            user_id LONG,
            event_time TIMESTAMP,
            event_type STRING,
            amount DOUBLE
        ) USING ICEBERG
    """
    )

    # Insert data in many small batches (creates many small, unsorted files)
    for _batch in range(20):
        _data4 = []
        for _i4 in range(100):
            _days_offset4 = random.randint(0, 365)
            _event_time4 = datetime(2023, 1, 1) + timedelta(days=_days_offset4)
            _user_id4 = random.randint(1, 1000)
            _event_type4 = random.choice(["click", "purchase", "view", "logout"])
            _amount4 = round(random.uniform(10, 500), 2)
            _data4.append((_user_id4, _event_time4, _event_type4, _amount4))

        _df4 = spark.createDataFrame(
            _data4, ["user_id", "event_time", "event_type", "amount"]
        )
        _df4.writeTo("iceberg_study.clustering_demo").append()

    # Get initial file count
    mo.md(
        """
    **Created unclustered table:**
    - 20 separate INSERT operations
    - Each creates 1-2 small files
    - Data is randomly distributed across files
    - Total: ~30 small files with random ordering
    """
    )
    return


@app.cell
def _(mo, spark_conn):
    # Check file count before clustering
    _df = mo.sql(
        f"""
        SELECT 
            COUNT(*) as file_count,
            AVG(record_count) as avg_rows_per_file,
            MIN(record_count) as min_rows,
            MAX(record_count) as max_rows
        FROM lakehouse.iceberg_study.clustering_demo.files
        """,
        engine=spark_conn
    )
    return


@app.cell
def _(mo, spark):
    # Now rewrite the data with proper sorting (clustering)
    # This simulates what Z-Ordering does
    spark.sql(
        """
        CALL lakehouse.system.rewrite_data_files(
            table => 'iceberg_study.clustering_demo',
            strategy => 'sort',
            sort_order => 'user_id ASC NULLS LAST, event_time ASC NULLS LAST',
            options => map(
                'min-input-files', '2'
            )
        )
    """
    )

    mo.md(
        """
    **Applied compaction with sorting:**
    - Combined small files into larger, optimally-sized files
    - Maintained sort order for clustering benefits
    - Reduced total file count
    - Improved data locality
    """
    )
    return


@app.cell
def _(mo, spark_conn):
    # Check file count after clustering
    _df = mo.sql(
        f"""
        SELECT 
            COUNT(*) as file_count,
            AVG(record_count) as avg_rows_per_file,
            MIN(record_count) as min_rows,
            MAX(record_count) as max_rows
        FROM lakehouse.iceberg_study.clustering_demo.files
        """,
        engine=spark_conn
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Statistics and File Pruning

    Iceberg stores column statistics (min/max) for each file. This enables pruning even without partitioning:

    ```
    Query: SELECT * FROM events WHERE user_id = 42

    File Statistics in Metadata:
    ┌─────────────────────────────────────────────────────────────┐
    │ File A: user_id range [1, 100]    → POTENTIAL MATCH        │
    │ File B: user_id range [101, 200]  → SKIP (42 not in range) │
    │ File C: user_id range [1, 50]     → POTENTIAL MATCH        │
    │ File D: user_id range [500, 600]  → SKIP (42 not in range) │
    └─────────────────────────────────────────────────────────────┘

    Result: Only read Files A and C (50% reduction!)
    ```

    This is called **min/max pruning** or **statistics-based pruning**.
    """)
    return


@app.cell
def _(mo, spark_conn):
    # Examine the column statistics in file metadata
    _df = mo.sql(
        f"""
        SELECT 
            file_path,
            record_count,
            lower_bounds,
            upper_bounds
        FROM lakehouse.iceberg_study.clustering_demo.files
        LIMIT 5
        """,
        engine=spark_conn
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 6: Partitioning Best Practices and Anti-Patterns

    ### Best Practices

    ✅ **DO:**
    1. **Partition on low-cardinality time columns** - `days(event_time)` is usually better than `hours(event_time)`
    2. **Use bucketing for high-cardinality columns** - `bucket(16, user_id)` not `user_id` directly
    3. **Keep partition count reasonable** - Aim for 100-10,000 partitions, not millions
    4. **Align partitions with query patterns** - Partition by what you filter on
    5. **Consider data size per partition** - Target 100MB-1GB per partition
    6. **Use partition evolution** - Add granularity as data grows
    7. **Combine with clustering** - Partition for file skipping, sort for local optimization

    ### Anti-Patterns

    ❌ **DON'T:**
    1. **Partition on high-cardinality columns** - Unique IDs create millions of tiny partitions
    2. **Over-partition** - Too many small partitions hurt query planning
    3. **Partition on frequently-modified columns** - Causes constant data movement
    4. **Ignore data distribution** - Skewed partitions (hot spots) limit parallelism
    5. **Use partitions for small tables** - Overhead not worth it for < 1GB tables
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Decision Tree: Should I Partition?

    ```
    Is your table > 10GB?
    │
    ├── NO → Don't partition (use sorting instead)
    │
    └── YES → Do queries filter on specific columns?
        │
        ├── NO → Don't partition (consider bucketing)
        │
        └── YES → Is the column low cardinality (< 10K distinct values)?
            │
            ├── YES → Use identity partition (category, country, etc.)
            │
            └── NO → Is it a timestamp?
                │
                ├── YES → Use time transform (days, months)
                │
                └── NO → Use bucketing (bucket(N, column))
    ```

    ### Common Partitioning Strategies

    | Data Type | Recommended Partitioning | Example |
    |-----------|-------------------------|---------|
    | Time-series | `days(event_time)` or `months(event_time)` | Logs, events |
    | Multi-tenant | `bucket(16, tenant_id)` + `days(event_time)` | SaaS applications |
    | Geographic | `country` or `region` | Location-based data |
    | Status/State | `status` (identity) | Order lifecycle |
    | High-cardinality | `bucket(64, user_id)` | User events |
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 7: Limitations and Edge Cases

    ### What Partitioning CAN'T Do

    1. **Doesn't create indexes** - Partitioning is not indexing
    2. **Doesn't guarantee sort order** - Use SortOrder for that
    3. **Can't partition on expressions** - Only predefined transforms
    4. **Doesn't help with point lookups** - Use a real database for OLTP
    5. **Can't remove partitions** - Evolution only allows adding

    ### Performance Trade-offs

    ```
    Partitioning Overhead:
    ┌─────────────────────────────────────────────────────────────┐
    │ ✓ Query planning: Slower (more metadata to read)            │
    │ ✗ Write amplification: Higher (may split batches)           │
    │ ✗ Storage overhead: More files (smaller on average)         │
    │ ✗ Compaction complexity: Harder to maintain                 │
    └─────────────────────────────────────────────────────────────┘

    Benefits must outweigh these costs!
    ```

    ### Small File Problem

    Over-partitioning creates the "small file problem":

    ```
    Good: 100 partitions × 10 files × 100MB = 100GB
    Bad:  10000 partitions × 10 files × 1MB = 100GB

    Problem: 10,000 partitions means:
    - 10,000× more metadata overhead
    - 10,000× more files to track
    - Listing operations become slow
    - Query planning takes longer
    ```

    **Solution:** Use bucketing or coarser granularity
    """)
    return


@app.cell
def _(datetime, mo, spark, timedelta):
    # Demonstrate the small file problem
    spark.catalog.setCurrentCatalog("lakehouse")
    spark.sql("DROP TABLE IF EXISTS iceberg_study.overpartitioned_demo")

    # Create an over-partitioned table (too many partitions for data size)
    spark.sql(
        """
        CREATE TABLE iceberg_study.overpartitioned_demo (
            user_id LONG,
            event_time TIMESTAMP,
            amount DOUBLE
        ) USING ICEBERG
        PARTITIONED BY (bucket(10, user_id), hours(event_time))
    """
    )

    # Insert small amount of data
    _data5 = []
    for _i5 in range(100):
        _event_time5 = datetime(2023, 1, 1) + timedelta(hours=_i5)
        _data5.append((_i5 % 50, _event_time5, 100.0))

    _df5 = spark.createDataFrame(_data5, ["user_id", "event_time", "amount"])
    _df5.writeTo("iceberg_study.overpartitioned_demo").append()

    mo.md(
        """
    **Created over-partitioned table:**
    - Partitioned by `bucket(10, user_id)` AND `hours(event_time)`
    - Only 100 rows of data
    - Results in many tiny partitions
    """
    )
    return


@app.cell
def _(mo, spark_conn):
    _df = mo.sql(
        f"""
        SELECT 
            COUNT(*) as partition_count,
            AVG(record_count) as avg_rows_per_partition,
            MIN(record_count) as min_rows,
            MAX(record_count) as max_rows
        FROM lakehouse.iceberg_study.overpartitioned_demo.partitions
        """,
        engine=spark_conn
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    **See the problem:** Many partitions with very few rows each. This creates overhead without benefit.

    ### Better Approach

    For 100 rows of data:
    - **Don't partition at all** - Not enough data to benefit
    - **Or use just `days(event_time)`** - Fewer partitions
    - **Add partitioning as data grows** via partition evolution
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 8: Summary and Key Takeaways

    ### Core Concepts

    1. **Hidden Partitioning**
       - Partition values derived from source columns
       - Not stored in data files, only in metadata
       - Transparent to queries - no schema pollution

    2. **Partition Transforms**
       - **Identity:** Direct column value (low cardinality)
       - **Bucket:** Hash distribution (high cardinality)
       - **Truncate:** Prefix-based (strings)
       - **Time:** Extract year/month/day/hour (timestamps)

    3. **Partition Evolution**
       - Add new partition fields without rewriting data
       - Maintains history of partition specs
       - Old data uses old spec, new data uses new spec

    4. **Partition Pruning**
       - Skip irrelevant files at query time
       - Works with equality and range predicates
       - Combines with min/max statistics for maximum benefit

    5. **Clustering/Z-Ordering**
       - Organizes data within files
       - Complements partitioning (use both!)
       - Requires periodic compaction maintenance

    ### Decision Checklist

    Before partitioning a table, ask:

    - [ ] Is the table > 10GB?
    - [ ] Do queries filter on specific columns?
    - [ ] Will this create < 10,000 partitions?
    - [ ] Is data distribution reasonably even?
    - [ ] Have I considered partition evolution for future needs?

    ### Performance Impact

    | Technique | Query Speed | Write Speed | Maintenance |
    |-----------|-------------|-------------|-------------|
    | No partitioning | Baseline | Fast | Minimal |
    | Good partitioning | 10-100× faster | Slower | Low |
    | Bad partitioning | Same or slower | Much slower | High |
    | With clustering | 2-10× faster | Slower | Medium |

    ### Next Steps

    - Benchmark your queries to identify filtering patterns
    - Start with coarse partitioning and evolve to finer granularity
    - Monitor partition sizes and file counts
    - Regular compaction to maintain clustering
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Appendix: Reference Tables

    ### Partition Transform Quick Reference

    | Transform | Column Types | Partition Count | Best For |
    |-----------|-------------|-----------------|----------|
    | `column` | Any | Cardinality of column | Categories, enums |
    | `bucket(N, column)` | String, int, long | Fixed to N | User IDs, high cardinality |
    | `truncate(L, column)` | String, int, long | Variable | Prefix-based grouping |
    | `years(column)` | Timestamp, Date | Years in data | Annual reports |
    | `months(column)` | Timestamp, Date | Months in data | Monthly analytics |
    | `days(column)` | Timestamp, Date | Days in data | Daily logs |
    | `hours(column)` | Timestamp | Hours in data | Hourly metrics |

    ### Predicate Support by Transform

    | Transform | = | <, <=, >, >= | IN | Range Pruning |
    |-----------|---|--------------|----|---------------|
    | Identity | ✓ | ✓ | ✓ | ✓ |
    | Bucket | ✓ | ✗ | ✓ | ✗ |
    | Truncate | ✓ | ✓* | ✓ | ✓* |
    | Year | ✓ | ✓ | ✓ | ✓ |
    | Month | ✓ | ✓ | ✓ | ✓ |
    | Day | ✓ | ✓ | ✓ | ✓ |
    | Hour | ✓ | ✓ | ✓ | ✓ |

    *For truncate: range pruning works but may be less effective

    ### Metadata Tables Reference

    | Table Suffix | Purpose |
    |-------------|---------|
    | `.partitions` | Partition values and statistics |
    | `$partition_specs` | Historical partition specifications |
    | `.files` | Data file list with column statistics |
    | `$snapshots` | Table snapshot history |
    | `$manifests` | Manifest file list |
    """)
    return


@app.cell
def _(mo, spark_conn):
    _df = mo.sql(
        f"""
        SELECT * FROM lakehouse.iceberg_study.events_partitioned
        """,
        engine=spark_conn
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
