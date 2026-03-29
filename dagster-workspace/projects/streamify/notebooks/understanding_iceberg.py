import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium", app_title="Understanding Iceberg")


@app.cell
def _():
    import logging
    import sys

    from pathlib import Path

    import marimo as mo

    sys.path.insert(0, str(Path(__file__).parent))

    from config import (
        create_duckdb_connection,
        create_iceberg_catalog,
        get_minio_config,
        get_polaris_config,
    )

    # Configure logging to see INFO messages
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    # Load configuration from environment
    polaris_config = get_polaris_config()
    minio_config = get_minio_config()
    return create_duckdb_connection, create_iceberg_catalog, mo


@app.cell
def _():

    store = get_s3_store()
    return store,

@app.cell
def _(create_iceberg_catalog):
    catalog = create_iceberg_catalog()
    return (catalog,)


@app.cell
def _(catalog):
    # Create iceberg_study namespace if it doesn't exist
    existing_namespaces = catalog.list_namespaces()
    if ("iceberg_study",) not in existing_namespaces:
        catalog.create_namespace(
            "iceberg_study", properties={"location": "s3://lakehouse/iceberg_study/"}
        )
        print("✓ Created namespace: iceberg_study")
    else:
        print("✓ Namespace already exists: iceberg_study")


@app.cell
def _(create_duckdb_connection):
    con = create_duckdb_connection()
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
    5. **Partitioning** - Hidden partitioning and evolution

    Let's begin!
    """)


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
    """)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Exploring Snapshots

    Let's look at the snapshots for the `bronze_listen_events` table.

    The `iceberg_snapshots()` function returns:
    - `sequence_number`: Monotonically increasing version number
    - `snapshot_id`: Unique identifier for this snapshot
    - `manifest_list`: Path to the manifest list file
    - `timestamp_ms`: When the snapshot was created
    """)


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        SELECT
            sequence_number,
            snapshot_id,
            manifest_list,
            timestamp_ms
        FROM iceberg_snapshots('lakehouse.streamify.bronze_listen_events')
        ORDER BY timestamp_ms DESC
        """,
        engine=con,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 2: Deep Dive - Iceberg Hierarchy

    Let's explore the complete Iceberg metadata hierarchy from top to bottom:

    ```
    Catalog → Metadata File → Snapshots → Manifest Lists → Manifest Files → Data Files
    ```
    """)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Level 0: The Catalog

    The **Catalog** is the entry point. It stores:
    - The location of the **current metadata file** for each table
    - Namespaces and their properties
    - Table identifiers

    We've already connected to the Polaris catalog. Let's list what it contains:
    """)


@app.cell
def _(catalog, mo):
    namespaces = catalog.list_namespaces()
    tables_streamify = catalog.list_tables("streamify")
    tables_iceberg_study = catalog.list_tables("iceberg_study")

    catalog_info = {
        "namespaces": [ns[0] for ns in namespaces],
        "tables_in_streamify": [t[1] for t in tables_streamify],
        "tables_in_iceberg_study": [t[1] for t in tables_iceberg_study],
    }

    mo.md(f"""
    **Catalog Contents:**

    - **Namespaces:** {catalog_info["namespaces"]}
    - **Tables in 'streamify':** {len(catalog_info["tables_in_streamify"])} tables
    - **Tables in 'iceberg_study':** {len(catalog_info["tables_in_iceberg_study"])} tables

    The catalog stores the pointer to the **current metadata file** for each table.
    """)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Level 1: The Metadata File

    The **metadata file** is the "brain" of Iceberg. It's a JSON file containing:
    - **Table schema** (all columns and types)
    - **Full snapshot history** (every snapshot ever created)
    - **Current snapshot reference** (which snapshot is current)
    - **Partition specifications**
    - **Table properties**

    Let's find the metadata file location and examine it:
    """)


@app.cell
def _(con):
    manifest_list = con.execute("""
        SELECT manifest_list
        FROM iceberg_snapshots('lakehouse.streamify.bronze_listen_events')
        ORDER BY timestamp_ms DESC
        LIMIT 1
    """).fetchone()[0]

    # Extract metadata directory from manifest_list path
    metadata_dir = manifest_list.rsplit("/", 1)[0]

    # Find the latest metadata.json file
    metadata_file_path = con.execute(f"""
        SELECT file
        FROM glob('{metadata_dir}/*metadata.json')
        ORDER BY file DESC
        LIMIT 1
    """).fetchone()[0]

    print(f"Metadata file: {metadata_file_path}")
    return (metadata_file_path,)


@app.cell
def _(metadata_file_path, mo):
    mo.md(f"""
    **Metadata File Location:**
    ```
    {metadata_file_path}
    ```

    This JSON file contains the complete table history and current state.
    """)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Metadata File History

    **Important**: Every time a commit happens (INSERT, UPDATE, DELETE), Iceberg creates a **NEW metadata file**!
    The catalog simply updates its pointer to the latest one. Let's see all metadata files:
    """)


@app.cell
def _(con, metadata_file_path, mo):
    # Extract metadata directory from current metadata file path
    _metadata_dir = metadata_file_path.rsplit("/", 1)[0]

    _df = mo.sql(
        f"""
        SELECT
            file,
            replace(file, '{_metadata_dir}/', '') as filename
        FROM glob('{_metadata_dir}/*metadata.json')
        ORDER BY file DESC
        """,
        engine=con,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    **Key Insight**:
    - Each commit creates a new metadata file (00001-, 00002-, 00053-, etc.)
    - The catalog atomically swaps its pointer to the new file
    - Old metadata files are kept for time-travel and auditing
    - This is how Iceberg achieves **atomic commits** and **snapshot isolation**
    """)


@app.cell
def _(con, metadata_file_path, mo):
    # Read the metadata JSON file content
    metadata_content = con.execute(f"""
        SELECT * FROM read_json('{metadata_file_path}')
    """).df()

    # Convert to dict for display
    metadata_dict = metadata_content.to_dict("records")[0]

    # Format the JSON nicely
    import json

    pretty_json = json.dumps(metadata_dict, indent=2, default=str)

    mo.md(f"""
    **Metadata File Contents (Raw JSON):**

    ```json
    {pretty_json[:3000]}...
    ```

    *(Showing first 3000 characters - the full file contains {len(pretty_json)} characters)*
    """)


@app.cell
def _(con, metadata_file_path, mo):
    _df = mo.sql(
        f"""
        SELECT
            "format-version" as format_version,
            "table-uuid" as table_uuid,
            location,
            "last-updated-ms" as last_updated_ms,
            "last-column-id" as last_column_id,
            "current-snapshot-id" as current_snapshot_id,
            len(snapshots) as num_snapshots
        FROM read_json('{metadata_file_path}')
        LIMIT 1
        """,
        engine=con,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    **Key Metadata Fields:**
    - `format-version`: Iceberg spec version (1 or 2)
    - `current-snapshot-id`: Points to the current snapshot
    - `num_snapshots`: Total number of snapshots in history
    - `location`: Base location of the table
    """)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Level 2: Snapshots

    **Snapshots** are immutable points-in-time views of the table.
    The metadata file contains an array of all snapshots, each with:
    - `snapshot_id`: Unique identifier
    - `timestamp_ms`: When it was created
    - `manifest_list`: Path to the manifest list file
    - `summary`: Operation statistics

    Let's see all snapshots for our table:
    """)


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        SELECT
            sequence_number,
            snapshot_id,
            manifest_list,
            timestamp_ms
        FROM iceberg_snapshots('lakehouse.streamify.bronze_listen_events')
        ORDER BY timestamp_ms DESC
        """,
        engine=con,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    **Snapshot Chain:**
    - Each snapshot has a `sequence_number` (incrementing)
    - The `manifest_list` column points to an Avro file containing all manifests for that snapshot
    - Snapshots form an immutable chain - you can time-travel to any snapshot!
    """)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Level 3: Manifest Lists

    Each **snapshot** points to a **manifest list** (Avro file). This file contains:
    - One entry per manifest file
    - Statistics about each manifest (file counts, row counts)
    - Partition information

    Let's explore the manifest list from the latest snapshot:
    """)


@app.cell
def _(con):
    # Get the manifest list path from the latest snapshot
    manifest_list_path = con.execute("""
        SELECT manifest_list
        FROM iceberg_snapshots('lakehouse.streamify.bronze_listen_events')
        ORDER BY timestamp_ms DESC
        LIMIT 1
    """).fetchone()[0]
    print(f"Manifest list: {manifest_list_path}")
    return (manifest_list_path,)


@app.cell
def _(con, manifest_list_path, mo):
    _df = mo.sql(
        f"""
        SELECT
            manifest_path,
            manifest_length,
            sequence_number,
            added_files_count,
            existing_files_count,
            deleted_files_count,
            added_rows_count
        FROM read_avro('{manifest_list_path}')
        LIMIT 5
        """,
        engine=con,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    **Manifest List Fields:**
    - `manifest_path`: Path to the manifest Avro file
    - `sequence_number`: Snapshot sequence number
    - `added_files_count`: New data files in this manifest
    - `existing_files_count`: Files carried over from previous snapshots
    - `deleted_files_count`: Files marked for deletion
    - `added_rows_count`: Total rows in added files
    """)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Level 4: Manifest Files

    Each **manifest file** is also an Avro file containing entries for individual **data files**.
    Each entry tracks:
    - `status`: 0=EXISTING, 1=ADDED, 2=DELETED
    - `snapshot_id`: Which snapshot added this file
    - `data_file`: Information about the actual data file (path, format, row count, etc.)

    Let's peek inside one manifest file:
    """)


@app.cell
def _(con, manifest_list_path):
    # Get the first manifest file path from the manifest list
    manifest_file_path = con.execute(f"""
        SELECT manifest_path
        FROM read_avro('{manifest_list_path}')
        LIMIT 1
    """).fetchone()[0]
    print(f"Manifest file: {manifest_file_path}")
    return (manifest_file_path,)


@app.cell
def _(con, manifest_file_path, mo):
    _df = mo.sql(
        f"""
        SELECT
            status,
            snapshot_id,
            sequence_number,
            data_file.file_path,
            data_file.file_format,
            data_file.record_count,
            data_file.file_size_in_bytes
        FROM read_avro('{manifest_file_path}')
        LIMIT 5
        """,
        engine=con,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    **Manifest File Entry Fields:**
    - `status`: 0=EXISTING, 1=ADDED, 2=DELETED
    - `snapshot_id`: Which snapshot added this file
    - `sequence_number`: Sequence number when added
    - `data_file.file_path`: Path to the actual Parquet file
    - `data_file.file_format`: File format (PARQUET)
    - `data_file.record_count`: Number of rows
    - `data_file.file_size_in_bytes`: File size
    """)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Level 5: Data Files

    Finally, the **data files** are the actual Parquet files containing the table data.
    These are stored in the `data/` directory of the table location.

    The manifests track which data files are:
    - **ADDED** (new data in this snapshot)
    - **EXISTING** (carried over from previous snapshots)
    - **DELETED** (removed in this snapshot - COW pattern)
    """)


@app.cell
def _(con, manifest_file_path, mo):
    _df = mo.sql(
        f"""
        SELECT
            data_file.file_path as data_file_path,
            data_file.content,
            data_file.file_format,
            data_file.record_count,
            data_file.file_size_in_bytes,
            CASE
                WHEN status = 0 THEN 'EXISTING'
                WHEN status = 1 THEN 'ADDED'
                WHEN status = 2 THEN 'DELETED'
            END as file_status
        FROM read_avro('{manifest_file_path}')
        LIMIT 5
        """,
        engine=con,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Summary: The Complete Hierarchy

    ```
    ┌─────────────────────────────────────┐
    │  Level 0: CATALOG                   │
    │  → Stores metadata file location    │
    └──────────────┬──────────────────────┘
                   │
                   ▼
    ┌─────────────────────────────────────┐
    │  Level 1: METADATA FILE (JSON)      │
    │  → All snapshots, schema, current   │
    └──────────────┬──────────────────────┘
                   │
                   ▼
    ┌─────────────────────────────────────┐
    │  Level 2: SNAPSHOTS                 │
    │  → Point-in-time view, manifest_list│
    └──────────────┬──────────────────────┘
                   │
                   ▼
    ┌─────────────────────────────────────┐
    │  Level 3: MANIFEST LIST (Avro)      │
    │  → List of manifests, statistics    │
    └──────────────┬──────────────────────┘
                   │
                   ▼
    ┌─────────────────────────────────────┐
    │  Level 4: MANIFEST FILES (Avro)     │
    │  → Data file entries, status        │
    └──────────────┬──────────────────────┘
                   │
                   ▼
    ┌─────────────────────────────────────┐
    │  Level 5: DATA FILES (Parquet)      │
    │  → Actual table data                │
    └─────────────────────────────────────┘
    ```

    **Key Takeaways:**
    1. **Immutable**: Snapshots, manifests, and data files are never modified
    2. **Copy-on-Write**: Updates create new snapshots with new data files
    3. **Time Travel**: You can query any snapshot by its ID
    4. **Atomic**: All changes are atomic at the snapshot level
    """)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## I/O Cost Analysis & Optimization Strategies

    ### Understanding the I/O Chain

    Every query follows this I/O chain:

    ```
    Query → Metadata File → Manifest List → Manifest(s) → Data File(s)
    ```

    **Minimum I/Os for a query:**
    1. Read metadata.json (1 I/O)
    2. Read manifest-list.avro (1 I/O)
    3. Read manifest-N.avro file(s) (N I/Os)
    4. Read data-XXX.parquet file(s) (M I/Os)

    **Total: 2 + N + M I/O operations**

    ### The Hidden Cost

    - **JSON parsing**: Metadata files can be MBs (especially with many snapshots)
    - **Multiple hops**: Even a simple query might touch 5-10 files before actual data
    - **Avro decoding**: Each manifest needs to be parsed

    ### The Solution: Statistics-Based Pruning

    **Manifest files contain min/max statistics for each column**, allowing the query engine to **skip data files entirely** without reading them!
    """)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Demonstration: Statistics-Based Pruning

    Let's look at the **lower_bounds** and **upper_bounds** in manifest entries. These statistics allow Iceberg to skip files that don't contain relevant data.
    """)


@app.cell(hide_code=True)
def _(con, mo):
    _df = mo.sql(
        """
        select * from lakehouse.streamify.bronze_listen_events
        """,
        engine=con,
    )


@app.cell
def _(con, manifest_file_path, mo):
    _df = mo.sql(
        f"""
        SELECT
            data_file.file_path,
            data_file.record_count,
            data_file.lower_bounds,
            data_file.upper_bounds
        FROM read_avro('{manifest_file_path}')
        LIMIT 10
        """,
        engine=con,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    **How Pruning Works:**

    Suppose you query: `WHERE user_id = 5000 AND timestamp > '2024-02-01'`

    Iceberg checks each manifest entry's bounds:
    - File A: user_id range [1000-9999], timestamp [Jan 1-31] → **SKIP!** (timestamp too old)
    - File B: user_id range [100-999], timestamp [Feb 1-28] → **SKIP!** (user_id out of range)
    - File C: user_id range [4000-6000], timestamp [Feb 1-28] → **READ!** (might contain data)

    **Without reading any Parquet files**, Iceberg eliminated 2 out of 3 files!
    """)


@app.cell
def _(con, manifest_file_path, mo):
    _df = mo.sql(
        f"""
        SELECT
            data_file.file_path,
            data_file.record_count,
            data_file.lower_bounds,
            data_file.upper_bounds,
            CASE
                WHEN data_file.record_count > 1000 THEN 'Large file'
                ELSE 'Small file'
            END as file_size_category
        FROM read_avro('{manifest_file_path}')
        ORDER BY data_file.record_count DESC
        LIMIT 5
        """,
        engine=con,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Real-World I/O Costs

    **Scenario: Table with 1000 data files, 50 snapshots**

    | Query Type | I/Os | Optimization |
    |------------|------|--------------|
    | **Full table scan** | 1 + 1 + 10 + 1000 = **1012 I/Os** | None |
    | **With good predicates** | 1 + 1 + 10 + 20 = **32 I/Os** | Statistics prune 98% |
    | **With caching** | 0 + 0 + 0 + 20 = **20 I/Os** | Metadata cached in memory |

    ### Optimization Strategies

    1. **Metadata Caching**: Keep metadata.json in memory (changes infrequently)
    2. **Manifest Caching**: Cache manifest files
    3. **Compaction**: Merge small files to reduce manifest size
    4. **Partitioning**: Organize data to maximize pruning
    5. **Predicate Pushdown**: Push filters as deep as possible

    ### The Tradeoff

    ```
    Cost:  Extra I/O for metadata + manifests
    Benefit:
      ✅ Time travel (query any historical snapshot)
      ✅ Atomic commits (all-or-nothing writes)
      ✅ Schema evolution (add columns without rewriting)
      ✅ Hidden partitioning (transparent optimization)
      ✅ Avoid full table scans via statistics
    ```

    **Bottom Line**: For analytical workloads (batch processing, large scans), the benefits outweigh the costs. For high-frequency, low-latency OLTP, consider alternatives.
    """)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 3: Hands-On Exercise - Creating Snapshots

    Let's create our own table and observe how snapshots are created!

    First, we'll create the `iceberg_study` namespace and a test table:
    """)


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        DROP TABLE IF EXISTS lakehouse.iceberg_study.study_table;
        CREATE TABLE lakehouse.iceberg_study.study_table (
            id INTEGER,
            fruit VARCHAR,
            quantity INTEGER
        );
        INSERT INTO lakehouse.iceberg_study.study_table VALUES
            (1, 'apple', 10),
            (2, 'banana', 20)
        """,
        engine=con,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### After First INSERT (Snapshot 1)

    Now let's check the data and snapshots:
    """)


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        SELECT * FROM lakehouse.iceberg_study.study_table
        """,
        engine=con,
    )


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        SELECT sequence_number, snapshot_id, timestamp_ms
        FROM iceberg_snapshots('lakehouse.iceberg_study.study_table')
        ORDER BY sequence_number
        """,
        engine=con,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### After Second INSERT (Snapshot 2)

    Let's insert more data and see how the snapshot chain grows:
    """)


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        INSERT INTO lakehouse.iceberg_study.study_table VALUES (3, 'cherry', 30)
        """,
        engine=con,
    )


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        SELECT *
        FROM iceberg_snapshots('lakehouse.iceberg_study.study_table')
        ORDER BY sequence_number
        """,
        engine=con,
    )


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        select
        	*
        from read_avro('s3://lakehouse/iceberg_study/study_table/metadata/snap-8107442448281716170-c1778c7d-008d-4504-811f-e22dd5f03a20.avro')
        """,
        engine=con,
    )


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        select
        	data_file.file_path as data_file_path,
            data_file.content,
            data_file.file_format,
            data_file.record_count,
            data_file.file_size_in_bytes,
            CASE
                WHEN status = 0 THEN 'EXISTING'
                WHEN status = 1 THEN 'ADDED'
                WHEN status = 2 THEN 'DELETED'
            END as file_status,
            *
        from read_avro('s3://lakehouse/iceberg_study/study_table/metadata/a9298af9-25f8-49b5-919b-5255b631883a-m0.avro')
        """,
        engine=con,
    )


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        select * from lakehouse.iceberg_study.study_table
        """,
        engine=con,
    )


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        delete from lakehouse.iceberg_study.study_table where fruit = 'cherry';
        """,
        engine=con,
    )


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        select * from READ_PARQUET('s3://lakehouse/iceberg_study/study_table/data/0a5bb0d9-bcf9-4d57-994f-36e6dc7525f5-deletes.parquet')
        """,
        engine=con,
    )


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        select * from READ_PARQUET('s3://lakehouse/iceberg_study/study_table/data/019d35fa-b514-79ec-9d53-708389da0486.parquet')
        """,
        engine=con,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    Notice:
    - Each new snapshot has an incremented sequence_number
    - This forms a chain of table versions over time
    """)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 4: Merge-on-Read (MOR) Demonstration

    Let's demonstrate Merge-on-Read (default) behavior with an UPDATE operation.

    First, create a test table:
    """)


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        DROP TABLE IF EXISTS lakehouse.iceberg_study.mor_example;

        CREATE TABLE lakehouse.iceberg_study.mor_example (
            id INTEGER,
            value VARCHAR
        );

        INSERT INTO lakehouse.iceberg_study.mor_example VALUES
            (1, 'original-1'),
            (2, 'original-2'),
            (3, 'original-3')
        """,
        engine=con,
    )


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        UPDATE lakehouse.iceberg_study.mor_example
        SET value = 'updated-2'
        WHERE id = 2;
        """,
        engine=con,
    )


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        SELECT * FROM lakehouse.iceberg_study.mor_example ORDER BY id
        """,
        engine=con,
    )


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        SELECT *
        FROM iceberg_snapshots('lakehouse.iceberg_study.mor_example')
        ORDER BY sequence_number
        """,
        engine=con,
    )


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        select * from
        read_avro("s3://lakehouse/iceberg_study/cow_example/metadata/snap-4187410115043606791-43e6369b-a1d2-4332-9cb7-9cac20efc376.avro")
        """,
        engine=con,
    )


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        select
        	data_file.file_path as data_file_path,
            data_file.content,
            data_file.file_format,
            data_file.record_count,
            data_file.file_size_in_bytes,
            CASE
                WHEN status = 0 THEN 'EXISTING'
                WHEN status = 1 THEN 'ADDED'
                WHEN status = 2 THEN 'DELETED'
            END as file_status,
            *
        from read_avro('s3://lakehouse/iceberg_study/cow_example/metadata/b80be7d0-02a6-45ec-929a-476a7c5dd427-m0.avro')
        """,
        engine=con,
    )


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        select * from READ_PARQUET("s3://lakehouse/iceberg_study/cow_example/data/019d3614-9a14-7e49-b1e9-94bb54d1c230.parquet")
        """,
        engine=con,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    **What happened?**
    - The UPDATE created a new snapshot
    - Data file was rewritten (MoR)
    - Old file marked as DELETED, new file as ADDED
    """)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # COW Demo
    """)


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        DROP TABLE IF EXISTS lakehouse.iceberg_study.cow_demo;

        CREATE TABLE lakehouse.iceberg_study.cow_demo (
            id INTEGER,
            value VARCHAR
        ) WITH (
            'write.update.mode' = 'copy-on-write',
            'write.delete.mode' = 'copy-on-write',
            'write.merge.mode' = 'copy-on-write'
        );

        INSERT INTO lakehouse.iceberg_study.cow_demo VALUES
            (1, 'original-1'),
            (2, 'original-2'),
            (3, 'original-3');
        """,
        engine=con,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    Now perform an UPDATE operation to trigger COW behavior:
    """)


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        UPDATE lakehouse.iceberg_study.cow_demo
        SET value = 'updated-2'
        WHERE id = 2;
        """,
        engine=con,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### COW Update Result

    Let's check the current data:
    """)


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        SELECT * FROM lakehouse.iceberg_study.cow_demo ORDER BY id
        """,
        engine=con,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    And the snapshot history:
    """)


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        SELECT sequence_number, snapshot_id, timestamp_ms
        FROM iceberg_snapshots('lakehouse.iceberg_study.cow_demo')
        ORDER BY sequence_number
        """,
        engine=con,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    **What happened in COW:**
    - The UPDATE created a new snapshot
    - The entire data file was **rewritten** with all 3 rows
    - Row 2 has the updated value, rows 1 and 3 are unchanged copies
    - Old file marked as DELETED (status 2), new file marked as ADDED (status 1)
    - No delete files created - just one new complete data file containing all rows

    Compare this to MOR (Module 4) which would create:
    - A positional delete file marking row 2 as deleted
    - A new data file with only the updated row
    - On read: must merge original + deletes + updates
    """)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""

    """)


if __name__ == "__main__":
    app.run()
