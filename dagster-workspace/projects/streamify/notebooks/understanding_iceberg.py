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


@app.cell
def _(con, mo):
    # Available columns: sequence_number, snapshot_id, manifest_list, timestamp_ms
    snapshots_df = con.execute("""
        SELECT 
            sequence_number,
            snapshot_id,
            manifest_list,
            timestamp_ms
        FROM iceberg_snapshots('lakehouse.streamify.bronze_listen_events')
        ORDER BY timestamp_ms DESC
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
    - `manifest_list`: Path to the manifest list file
    - `timestamp_ms`: When the snapshot was created
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 2: Understanding Metadata with iceberg_metadata()

    The `iceberg_metadata()` function returns information about files in the table:
    - `manifest_path`: Path to manifest file
    - `status`: File status (ADDED, EXISTING, DELETED)
    - `file_path`: Path to data file
    - `file_format`: File format (PARQUET, etc.)
    - `record_count`: Number of records
    """)
    return


@app.cell
def _(con, mo):
    metadata_df = con.execute("""
        SELECT 
            manifest_path,
            status,
            file_path,
            file_format,
            record_count
        FROM iceberg_metadata('lakehouse.streamify.bronze_listen_events')
        LIMIT 10
    """).df()

    mo.md(f"""
    ### Iceberg Metadata Files

    ```
    {metadata_df.to_string()}
    ```

    **Key Insights:**
    - `status`: Shows if file is ADDED, EXISTING, or DELETED
    - `manifest_path`: References the manifest containing this file
    - `file_path`: The actual data file location
    - `record_count`: Number of rows in the file
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 3: Hands-On Exercise - Creating Snapshots

    Let's create our own table and observe how snapshots are created!
    """)
    return


@app.cell
def _(con):
    # Drop if exists, then create
    con.execute("DROP TABLE IF EXISTS lakehouse.streamify.iceberg_study")
    con.execute("""
        CREATE TABLE lakehouse.streamify.iceberg_study (
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
    snapshots = con.execute("""
        SELECT sequence_number, snapshot_id, timestamp_ms
        FROM iceberg_snapshots('lakehouse.streamify.iceberg_study')
        ORDER BY sequence_number
    """).df()

    # Check the data
    data = con.execute("""
        SELECT * FROM lakehouse.streamify.iceberg_study
    """).df()

    mo.md(f"""
    ### After First INSERT (Snapshot 1)

    **Data in table:**
    ```
    {data.to_string()}
    ```

    **Snapshots:**
    ```
    {snapshots.to_string()}
    ```
    """)
    return


@app.cell
def _(con, mo):
    con.execute("""
        INSERT INTO lakehouse.streamify.iceberg_study VALUES
            (3, 'cherry', 30)
    """)

    # Check updated snapshots
    snapshots2 = con.execute("""
        SELECT sequence_number, snapshot_id, timestamp_ms
        FROM iceberg_snapshots('lakehouse.streamify.iceberg_study')
        ORDER BY sequence_number
    """).df()

    mo.md(f"""
    ### After Second INSERT (Snapshot 2)

    **Snapshots:**
    ```
    {snapshots2.to_string()}
    ```

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
    """)
    return


@app.cell
def _(con):
    con.execute("DROP TABLE IF EXISTS lakehouse.streamify.cow_example")
    con.execute("""
        CREATE TABLE lakehouse.streamify.cow_example (
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

    print("✓ Created COW example table")
    return


@app.cell
def _(con, mo):
    con.execute("""
        UPDATE lakehouse.streamify.cow_example
        SET value = 'updated-2'
        WHERE id = 2
    """)

    # Check current data
    cow_data = con.execute("""
        SELECT * FROM lakehouse.streamify.cow_example ORDER BY id
    """).df()

    # Check snapshots
    cow_snapshots = con.execute("""
        SELECT sequence_number, snapshot_id, timestamp_ms
        FROM iceberg_snapshots('lakehouse.streamify.cow_example')
        ORDER BY sequence_number
    """).df()

    mo.md(f"""
    ### COW Update Result

    **Current Data:**
    ```
    {cow_data.to_string()}
    ```

    **Snapshot History:**
    ```
    {cow_snapshots.to_string()}
    ```

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
