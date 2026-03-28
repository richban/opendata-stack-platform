import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")


@app.cell
def _():
    # .env.polaris is loaded by direnv (.envrc) — all POLARIS_* vars are already
    # in the environment. No load_dotenv needed.
    import os

    import duckdb
    import marimo as mo
    import sqlalchemy

    from pyiceberg.catalog.rest import RestCatalog

    POLARIS_CLIENT_ID = os.getenv("POLARIS_CLIENT_ID")
    POLARIS_CLIENT_SECRET = os.getenv("POLARIS_CLIENT_SECRET")
    POLARIS_CATALOG = os.getenv("POLARIS_CATALOG", "lakehouse")
    POLARIS_URI = os.getenv("POLARIS_URI", "http://localhost:8181/api/catalog")

    MINIO_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000")
    MINIO_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    MINIO_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

    print(f"POLARIS_CLIENT_ID:     {POLARIS_CLIENT_ID}")
    print(f"POLARIS_CLIENT_SECRET: {POLARIS_CLIENT_SECRET}")
    print(f"POLARIS_URI:     {POLARIS_URI}")
    print(f"POLARIS_CATALOG: {POLARIS_CATALOG}")
    print(f"POLARIS_CLIENT_ID set: {bool(POLARIS_CLIENT_ID)}")
    print(f"MINIO_ENDPOINT:    {MINIO_ENDPOINT}")
    print(f"MINIO_KEY:         {MINIO_KEY}")
    print(f"MINIO_SECRET:      {MINIO_SECRET}")
    return (
        MINIO_ENDPOINT,
        MINIO_KEY,
        MINIO_SECRET,
        POLARIS_CATALOG,
        POLARIS_CLIENT_ID,
        POLARIS_CLIENT_SECRET,
        POLARIS_URI,
        RestCatalog,
        duckdb,
        mo,
        os,
    )


@app.cell
def _():
    from obstore.store import S3Store

    store = S3Store("lakehouse",
        access_key_id="minioadmin",
        secret_access_key="minioadmin",
        endpoint_url="http://localhost:9000",
    )
    return


@app.cell
def _(RestCatalog, os):

    catalog = RestCatalog(
        name=os.getenv("POLARIS_CATALOG", "lakehouse"),
        **{
            "uri": os.getenv("POLARIS_URI", "http://localhost:8181/api/catalog"),
            "warehouse": os.getenv("POLARIS_CATALOG", "lakehouse"),
            "credential": f"{os.getenv('POLARIS_CLIENT_ID')}:{os.getenv('POLARIS_CLIENT_SECRET')}",
            "scope": "PRINCIPAL_ROLE:ALL",
            "header.X-Iceberg-Access-Delegation": "vended-credentials",
        },
    )

    print(f"Namespaces: {catalog.list_namespaces()}")
    print(f"Tables:     {catalog.list_tables('streamify')}")
    return


@app.cell
def _(
    MINIO_ENDPOINT,
    MINIO_KEY,
    MINIO_SECRET,
    POLARIS_CATALOG,
    POLARIS_CLIENT_ID,
    POLARIS_CLIENT_SECRET,
    POLARIS_URI,
    duckdb,
):
    # DuckDB — native Iceberg REST catalog via ATTACH
    # Docs: https://duckdb.org/docs/stable/core_extensions/iceberg/iceberg_rest_catalogs#polaris

    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("INSTALL httpfs;  LOAD httpfs;")

    # OAuth2 secret — DuckDB exchanges client_id/secret for a bearer token.
    # OAUTH2_SERVER_URI needed because Polaris puts its token endpoint at
    # /api/catalog/v1/oauth/tokens, not at the catalog root.
    con.execute(f"""
        CREATE OR REPLACE SECRET polaris_secret (
            TYPE              iceberg,
            CLIENT_ID         '{POLARIS_CLIENT_ID}',
            CLIENT_SECRET     '{POLARIS_CLIENT_SECRET}',
            OAUTH2_SCOPE      'PRINCIPAL_ROLE:ALL',
            OAUTH2_SERVER_URI '{POLARIS_URI}/v1/oauth/tokens'
        )
    """)

    # S3 secret for MinIO.
    # Polaris catalog config has stsUnavailable=true, so vended (STS) credentials
    # won't work. We use static MinIO credentials scoped to s3://lakehouse/.
    _minio_host = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    con.execute(f"""
        CREATE OR REPLACE SECRET minio_secret (
            TYPE      s3,
            KEY_ID    '{MINIO_KEY}',
            SECRET    '{MINIO_SECRET}',
            ENDPOINT  '{_minio_host}',
            SCOPE     's3://lakehouse',
            URL_STYLE 'path',
            USE_SSL   false
        )
    """)

    # ATTACH: ACCESS_DELEGATION_MODE 'none' is critical here.
    # Default is 'vended_credentials' which makes DuckDB request temporary STS creds
    # from Polaris — but our Polaris has stsUnavailable=true and MinIO runs on a
    # Docker-internal hostname (minio:9000) unreachable from the host.
    # 'none' tells DuckDB to use minio_secret directly for all s3:// data file reads.
    con.execute(f"""
        ATTACH '{POLARIS_CATALOG}' AS lakehouse (
            TYPE                   iceberg,
            ENDPOINT               '{POLARIS_URI}',
            SECRET                 'polaris_secret',
            ACCESS_DELEGATION_MODE 'none'
        )
    """)

    print("DuckDB attached to Polaris ✓")
    print(con.execute("SHOW ALL TABLES").df().to_string())
    return (con,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # Understanding Apache Iceberg's Consistency Model - Interactive Study Guide

    This notebook is an interactive companion to [Jack Vanlightly's excellent article](https://jack-vanlightly.com/analyses/2024/7/30/understanding-apache-icebergs-consistency-model-part1).

    ## Learning Objectives

    By the end of this guide, you'll understand:
    1. **Iceberg Architecture** - Metadata layer vs Data layer
    2. **Snapshots** - How table versions work
    3. **Manifest Files** - Tracking additions and deletions
    4. **COW vs MOR** - Copy-on-write vs Merge-on-read
    5. **Compaction** - Optimizing table performance
    6. **Partitioning** - Hidden partitioning and evolution

    Let's begin!
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 1: Iceberg Architecture Overview

    Iceberg splits table files between:
    - **Metadata Layer**: Snapshots, manifest lists, manifest files
    - **Data Layer**: Parquet/ORC/Avro data files

    ### Key Concept: The Catalog

    The catalog stores the location of the **current metadata file**. Each write creates:
    1. New data files
    2. New manifest files (listing data files)
    3. New manifest list (listing manifest files)
    4. New metadata file (listing all snapshots)
    5. Atomic commit to catalog (compare-and-swap)

    Let's explore this with a real table!
    """)
    return


@app.cell
def _(con, mo):
    # First, let's see what tables exist
    tables_df = con.execute("SHOW ALL TABLES").df()
    mo.md(f"""
    ### Available Tables

    ```
    {tables_df.to_string()}
    ```
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 2: Understanding Snapshots

    A **snapshot** represents the state of a table at a point in time.

    Each snapshot contains:
    - **Manifest List**: Points to all manifest files
    - **Manifest Files**: List data files and their status (ADDED/EXISTING/DELETED)
    - **Data Files**: The actual Parquet/ORC files with data

    Let's examine the snapshots of a real table.
    """)
    return


@app.cell
def _(con, mo):
    # Explore snapshots
    snapshots_df = con.execute("""
        SELECT 
            sequence_number,
            snapshot_id,
            parent_id,
            manifest_list,
            committed_at,
            operation,
            summary
        FROM iceberg_snapshots('lakehouse.streamify.bronze_listen_events')
        ORDER BY committed_at DESC
    """).df()

    mo.md(f"""
    ### Table: `bronze_listen_events` Snapshots

    Each row represents a snapshot (version) of the table:

    ```
    {snapshots_df.to_string()}
    ```

    **Key Fields:**
    - `sequence_number`: Monotonically increasing version number
    - `snapshot_id`: Unique identifier for this snapshot
    - `parent_id`: Reference to previous snapshot (forms a chain)
    - `manifest_list`: Path to the manifest list file
    - `operation`: Type of operation (append, overwrite, etc.)
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 3: Manifest Lists and Manifest Files

    ### Manifest List
    The manifest list file contains one entry per manifest file, with metadata about each manifest.

    ### Manifest File
    Each manifest file contains entries that reference data files. Each entry has:
    - **Status**: ADDED, EXISTING, or DELETED
    - **Data File Path**: Location of the data file
    - **Partition Info**: Partition values
    - **Statistics**: Row count, null count, min/max values

    Let's inspect these files directly!
    """)
    return


@app.cell
def _(con, mo):
    # Explore manifest files for the latest snapshot
    manifests_df = con.execute("""
        SELECT 
            manifest_path,
            manifest_sequence_number,
            manifest_content,
            added_data_files_count,
            existing_data_files_count,
            deleted_data_files_count,
            added_rows_count,
            deleted_rows_count
        FROM iceberg_manifests('lakehouse.streamify.bronze_listen_events')
    """).df()

    mo.md(f"""
    ### Manifest Files for Latest Snapshot

    ```
    {manifests_df.to_string()}
    ```

    **Key Insights:**
    - `manifest_content`: 0 = DATA files, 1 = DELETE files (for MOR)
    - `added_*`: Files/rows added in this snapshot
    - `deleted_*`: Files/rows deleted in this snapshot
    - `existing_*`: Files carried over from previous snapshots
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 4: Hands-On Exercise - Creating Snapshots

    Let's create our own table and observe how snapshots are created!

    We'll:
    1. Create a new Iceberg table
    2. Insert data (creates snapshot 1)
    3. Insert more data (creates snapshot 2)
    4. Delete some data (creates snapshot 3)
    5. Observe the manifest changes

    This will help you understand how ADDED, EXISTING, and DELETED statuses work.
    """)
    return


@app.cell
def _(con):
    # Create a test table for learning
    con.execute("""
        CREATE OR REPLACE TABLE lakehouse.streamify.iceberg_study (
            id INTEGER,
            fruit VARCHAR,
            quantity INTEGER
        )
    """)

    # Insert first batch
    con.execute("""
        INSERT INTO lakehouse.streamify.iceberg_study VALUES
            (1, 'apple', 10),
            (2, 'banana', 20)
    """)

    print("✓ Created table and inserted first 2 rows")
    return


@app.cell
def _(con, mo):
    # Check snapshots after first insert
    snapshots_1 = con.execute("""
        SELECT 
            sequence_number,
            snapshot_id,
            parent_id,
            operation,
            committed_at
        FROM iceberg_snapshots('lakehouse.streamify.iceberg_study')
        ORDER BY sequence_number
    """).df()

    mo.md(f"""
    ### After First INSERT (Snapshot 1)

    ```
    {snapshots_1.to_string()}
    ```
    """)
    return


@app.cell
def _(con, mo):
    # Insert second batch
    con.execute("""
        INSERT INTO lakehouse.streamify.iceberg_study VALUES
            (3, 'cherry', 30)
    """)

    # Check snapshots after second insert
    snapshots_2 = con.execute("""
        SELECT 
            sequence_number,
            snapshot_id,
            parent_id,
            operation,
            committed_at
        FROM iceberg_snapshots('lakehouse.streamify.iceberg_study')
        ORDER BY sequence_number
    """).df()

    mo.md(f"""
    ### After Second INSERT (Snapshot 2)

    ```
    {snapshots_2.to_string()}
    ```

    Notice:
    - `parent_id` of snapshot 2 points to snapshot 1
    - This forms a chain of table versions
    """)
    return


@app.cell
def _(con, mo):
    # Let's look at the manifest files for both snapshots
    manifests_study = con.execute("""
        SELECT 
            sequence_number,
            manifest_path,
            added_data_files_count,
            existing_data_files_count,
            deleted_data_files_count,
            added_rows_count
        FROM iceberg_manifests('lakehouse.streamify.iceberg_study')
        ORDER BY sequence_number, manifest_path
    """).df()

    mo.md(f"""
    ### Manifest Files Across Snapshots

    ```
    {manifests_study.to_string()}
    ```

    **Exercise Questions:**
    1. How many manifest files exist for snapshot 1?
    2. How many for snapshot 2?
    3. What does `added_data_files_count` tell you?
    4. What does `existing_data_files_count` tell you?
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 5: Copy-on-Write (COW) vs Merge-on-Read (MOR)

    Iceberg supports two modes for handling updates and deletes:

    ### Copy-on-Write (COW)
    - **Write**: When updating/deleting rows, entire data files are rewritten
    - **Read**: Fast - just read data files
    - **Use case**: Read-heavy workloads, occasional updates

    ### Merge-on-Read (MOR)
    - **Write**: Creates delete files (position or equality based) without rewriting data
    - **Read**: Must merge delete files with data files
    - **Use case**: Write-heavy workloads, frequent updates

    Let's demonstrate this with examples!
    """)
    return


@app.cell
def _(con):
    # Create a COW table and demonstrate an update
    con.execute("""
        CREATE OR REPLACE TABLE lakehouse.streamify.cow_example (
            id INTEGER,
            value VARCHAR
        )
    """)

    con.execute("""
        INSERT INTO lakehouse.streamify.cow_example VALUES
            (1, 'original-1'),
            (2, 'original-2'),
            (3, 'original-3')
    """)

    print("✓ Created COW example table with 3 rows")
    return


@app.cell
def _(con, mo):
    # Perform an update (will trigger COW behavior)
    con.execute("""
        UPDATE lakehouse.streamify.cow_example
        SET value = 'updated-2'
        WHERE id = 2
    """)

    # Check what happened
    cow_manifests = con.execute("""
        SELECT 
            sequence_number,
            operation,
            added_data_files_count,
            deleted_data_files_count,
            added_rows_count,
            deleted_rows_count
        FROM iceberg_manifests('lakehouse.streamify.cow_example')
        ORDER BY sequence_number
    """).df()

    mo.md(f"""
    ### COW Update Result

    ```
    {cow_manifests.to_string()}
    ```

    **What happened?**
    - The UPDATE operation created a new snapshot
    - Data file containing row id=2 was **rewritten**
    - Old file marked as DELETED
    - New file marked as ADDED
    - Even though only 1 row changed, entire file was rewritten!
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 6: Partitioning and Hidden Partitioning

    Iceberg uses **hidden partitioning** - you don't need to create separate partition columns!

    ### Transform Functions:
    - `year(ts)`, `month(ts)`, `day(ts)`, `hour(ts)` - Time-based
    - `bucket(n, col)` - Hash into n buckets
    - `truncate(col, len)` - String truncation
    - `identity(col)` - Direct value

    ### Partition Evolution:
    You can change partition specs without rewriting existing data!

    Let's create a partitioned table and observe.
    """)
    return


@app.cell
def _(con):
    # Create a partitioned table using hidden partitioning
    con.execute("""
        CREATE OR REPLACE TABLE lakehouse.streamify.partitioned_events (
            event_id BIGINT,
            user_id VARCHAR,
            event_time TIMESTAMP,
            event_type VARCHAR,
            amount DECIMAL(10,2)
        )
    """)

    # Add data with different timestamps
    con.execute("""
        INSERT INTO lakehouse.streamify.partitioned_events VALUES
            (1, 'user1', '2024-01-15 10:00:00', 'click', 10.00),
            (2, 'user2', '2024-01-15 11:00:00', 'view', NULL),
            (3, 'user1', '2024-01-16 10:00:00', 'click', 20.00),
            (4, 'user3', '2024-02-01 09:00:00', 'purchase', 100.00)
    """)

    print("✓ Created partitioned table")
    return


@app.cell
def _(con, mo):
    # Now let's partition by day(event_time)
    con.execute("""
        ALTER TABLE lakehouse.streamify.partitioned_events
        ADD PARTITION FIELD day(event_time)
    """)

    # Check partition spec
    partitions_df = con.execute("""
        SELECT 
            partition_id,
            partition_field,
            partition_transform
        FROM iceberg_partitions('lakehouse.streamify.partitioned_events')
    """).df()

    mo.md(f"""
    ### Partition Specification

    ```
    {partitions_df.to_string()}
    ```

    **Key Point:**
    The `event_time` column itself remains unchanged. The partition is **hidden**!
    Queries filtering on `event_time` automatically benefit from partition pruning.
    """)
    return


@app.cell
def _(con, mo):
    # Insert more data after partitioning
    con.execute("""
        INSERT INTO lakehouse.streamify.partitioned_events VALUES
            (5, 'user4', '2024-03-01 08:00:00', 'view', NULL)
    """)

    # Check files in each partition
    partition_files = con.execute("""
        SELECT 
            partition,
            record_count,
            file_size_bytes,
            file_path
        FROM iceberg_files('lakehouse.streamify.partitioned_events')
        ORDER BY partition
    """).df()

    mo.md(f"""
    ### Data Files by Partition

    ```
    {partition_files.to_string()}
    ```

    Notice how data files are organized by partition values!
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 7: Compaction

    Compaction rewrites small files into fewer, larger files to improve read performance.

    ### Types:
    1. **Data File Compaction**: Merge small data files
    2. **Manifest Compaction**: Merge small manifest files

    ### Strategies:
    - **Bin-pack**: Efficient, no sorting
    - **Sort**: Reorganize data for better locality
    - **Z-Ordering**: Multi-dimensional clustering

    Let's demonstrate compaction!
    """)
    return


@app.cell
def _(con):
    # Create a table with many small files
    con.execute("""
        CREATE OR REPLACE TABLE lakehouse.streamify.compaction_demo (
            id INTEGER,
            category VARCHAR,
            value DOUBLE
        )
    """)

    # Insert multiple small batches (simulating streaming inserts)
    for i in range(5):
        con.execute(f"""
            INSERT INTO lakehouse.streamify.compaction_demo VALUES
                ({i * 2 + 1}, 'A', {i * 1.1}),
                ({i * 2 + 2}, 'B', {i * 2.2})
        """)

    print("✓ Created table with 5 small data files")
    return


@app.cell
def _(con, mo):
    # Check file count before compaction
    files_before = con.execute("""
        SELECT 
            COUNT(*) as file_count,
            SUM(record_count) as total_rows,
            AVG(file_size_bytes) as avg_file_size
        FROM iceberg_files('lakehouse.streamify.compaction_demo')
    """).df()

    mo.md(f"""
    ### Before Compaction

    ```
    {files_before.to_string()}
    ```

    Many small files hurt read performance!
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ### Compaction in Practice

    In production Iceberg tables, you would run:

    ```sql
    -- Spark SQL
    OPTIMIZE lakehouse.streamify.compaction_demo

    -- Or with sorting
    OPTIMIZE lakehouse.streamify.compaction_demo
    ZORDER BY (category)
    ```

    **What compaction does:**
    1. Reads all small files in a partition
    2. Rewrites them as fewer, larger files
    3. Updates manifests atomically
    4. Old files marked for deletion (can be cleaned up later)

    ### Benefits:
    - Reduces files to scan during reads
    - Improves query planning
    - Better compression ratios
    - Reduced metadata overhead
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 8: Understanding Metadata Files

    Let's peek behind the curtain and look at actual metadata files!

    The metadata file contains:
    - Table schema
    - Partition specs
    - All snapshots
    - Current snapshot reference
    - Properties

    Let's explore the metadata location.
    """)
    return


@app.cell
def _(con, mo):
    # Get metadata location
    metadata_info = con.execute("""
        SELECT 
            table_type,
            metadata_location,
            location
        FROM information_schema.tables
        WHERE table_schema = 'streamify'
          AND table_name = 'iceberg_study'
    """).df()

    mo.md(f"""
    ### Metadata Location

    ```
    {metadata_info.to_string()}
    ```

    The `metadata_location` points to the current metadata JSON file.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 9: Concurrency Control Summary

    Iceberg's consistency model is built on:

    ### 1. Atomic Catalog Commits
    - Uses compare-and-swap (CAS) operations
    - Metadata files have UUIDs to prevent collisions
    - Multiple writers can compete, only one succeeds

    ### 2. Optimistic Concurrency Control
    - Writers check for conflicts before committing
    - If another writer committed first, retry with updated base

    ### 3. Snapshot Isolation
    - Readers see consistent snapshots
    - Writers don't block readers
    - Time-travel between snapshots

    ### Write Operations:
    - **AppendFiles**: Simple data addition
    - **OverwriteFiles**: COW updates/deletes
    - **RowDelta**: MOR updates/deletes
    - **RewriteFiles**: Compaction

    ---

    ## Summary

    You've learned:

    1. ✅ **Iceberg Architecture**: Metadata layer + Data layer
    2. ✅ **Snapshots**: Immutable table versions with parent-child relationships
    3. ✅ **Manifests**: Track file status (ADDED/EXISTING/DELETED)
    4. ✅ **COW vs MOR**: Trade-offs between write and read amplification
    5. ✅ **Partitioning**: Hidden transforms without separate columns
    6. ✅ **Compaction**: Optimizing file sizes for read performance
    7. ✅ **Concurrency**: Atomic commits with optimistic locking

    ### Next Steps:

    - Read [Part 2 of the article](https://jack-vanlightly.com/analyses/2024/8/5/apache-icebergs-consistency-model-part-2) for deeper concurrency details
    - Experiment with time-travel queries
    - Try different partition transforms
    - Set up compaction jobs in your pipeline
    """)
    return


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Appendix: Useful Queries

    Keep these handy for exploring Iceberg tables!

    ```sql
    -- List all snapshots
    SELECT * FROM iceberg_snapshots('table_name');

    -- List manifest files
    SELECT * FROM iceberg_manifests('table_name');

    -- List data files
    SELECT * FROM iceberg_files('table_name');

    -- Query as of specific snapshot
    SELECT * FROM table_name VERSION AS OF snapshot_id;

    -- Query as of specific time
    SELECT * FROM table_name TIMESTAMP AS OF '2024-01-01';

    -- Table history
    SELECT * FROM iceberg_history('table_name');

    -- Partition info
    SELECT * FROM iceberg_partitions('table_name');
    ```
    """)
    return


if __name__ == "__main__":
    app.run()
