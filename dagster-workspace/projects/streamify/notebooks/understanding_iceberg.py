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

    store = S3Store(
        "lakehouse",
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
    5. **Partitioning** - Hidden partitioning and evolution

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
    """)
    return


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
    return


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
    return


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
    return


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
    return


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
    return


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
    return


@app.cell
def _(con):
    # The catalog DOES store the metadata file location (via metadata_location property)
    # In production: table = catalog.load_table("..."); metadata_file = table.metadata_location
    # However, our Polaris catalog uses vended credentials which requires extra setup.
    # For this demo, we find the metadata file via SQL from the snapshot's manifest_list path.

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
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Metadata File History

    **Important**: Every time a commit happens (INSERT, UPDATE, DELETE), Iceberg creates a **NEW metadata file**!
    The catalog simply updates its pointer to the latest one. Let's see all metadata files:
    """)
    return


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
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    **Key Insight**:
    - Each commit creates a new metadata file (00001-, 00002-, 00053-, etc.)
    - The catalog atomically swaps its pointer to the new file
    - Old metadata files are kept for time-travel and auditing
    - This is how Iceberg achieves **atomic commits** and **snapshot isolation**
    """)
    return


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
    return


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
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    **Key Metadata Fields:**
    - `format-version`: Iceberg spec version (1 or 2)
    - `current-snapshot-id`: Points to the current snapshot
    - `num_snapshots`: Total number of snapshots in history
    - `location`: Base location of the table
    """)
    return


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
    return


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
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    **Snapshot Chain:**
    - Each snapshot has a `sequence_number` (incrementing)
    - The `manifest_list` column points to an Avro file containing all manifests for that snapshot
    - Snapshots form an immutable chain - you can time-travel to any snapshot!
    """)
    return


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
    return


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
    return


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
    return


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
    return


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
    return


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
    return


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
    return


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
        engine=con
    )
    return


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
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 3: Hands-On Exercise - Creating Snapshots

    Let's create our own table and observe how snapshots are created!

    First, we'll create the `iceberg_study` namespace and a test table:
    """)
    return


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
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### After First INSERT (Snapshot 1)

    Now let's check the data and snapshots:
    """)
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        SELECT * FROM lakehouse.iceberg_study.study_table
        """,
        engine=con,
    )
    return


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
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### After Second INSERT (Snapshot 2)

    Let's insert more data and see how the snapshot chain grows:
    """)
    return


@app.cell
def _(con):
    con.execute("""
        INSERT INTO lakehouse.iceberg_study.study_table VALUES
            (3, 'cherry', 30)
    """)
    print("✓ Inserted row 3")
    return


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
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    Notice:
    - Each new snapshot has an incremented sequence_number
    - This forms a chain of table versions over time
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 4: Copy-on-Write (COW) Demonstration

    Let's demonstrate Copy-on-Write behavior with an UPDATE operation.

    First, create a test table:
    """)
    return


@app.cell
def _(con):
    con.execute("DROP TABLE IF EXISTS lakehouse.iceberg_study.cow_example")
    con.execute("""
        CREATE TABLE lakehouse.iceberg_study.cow_example (
            id INTEGER,
            value VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO lakehouse.iceberg_study.cow_example VALUES
            (1, 'original-1'),
            (2, 'original-2'),
            (3, 'original-3')
    """)
    print("✓ Created COW example table with 3 rows")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    Now perform an UPDATE operation:
    """)
    return


@app.cell
def _(con):
    con.execute("""
        UPDATE lakehouse.iceberg_study.cow_example
        SET value = 'updated-2'
        WHERE id = 2
    """)
    print("✓ Updated row 2")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### COW Update Result

    Let's check the current data:
    """)
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        SELECT * FROM lakehouse.iceberg_study.cow_example ORDER BY id
        """,
        engine=con,
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    And the snapshot history:
    """)
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        SELECT sequence_number, snapshot_id, timestamp_ms
        FROM iceberg_snapshots('lakehouse.iceberg_study.cow_example')
        ORDER BY sequence_number
        """,
        engine=con,
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    **What happened?**
    - The UPDATE created a new snapshot
    - Data file was rewritten (Copy-on-Write)
    - Old file marked as DELETED, new file as ADDED
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 5: Summary

    You've learned:

    1. ✅ **Iceberg Architecture**: Metadata layer + Data layer
    2. ✅ **Snapshots**: Use `iceberg_snapshots()` to see version history
    3. ✅ **Metadata**: Use `iceberg_metadata()` to see file information
    4. ✅ **COW**: Updates rewrite entire files
    5. ✅ **Time Travel**: Access historical versions via snapshots

    ### Available DuckDB Iceberg Functions:

    ```sql
    -- List snapshots
    SELECT * FROM iceberg_snapshots('table_name');

    -- View metadata
    SELECT * FROM iceberg_metadata('table_name');

    -- Query partition stats
    SELECT * FROM iceberg_partition_stats('table_name');

    -- Time travel
    SELECT * FROM table_name VERSION AS OF snapshot_id;
    ```
    """)
    return


if __name__ == "__main__":
    app.run()
