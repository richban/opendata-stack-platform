# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "marimo",
#     "duckdb",
#     "pyiceberg",
#     "sqlglot",
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
app = marimo.App(width="medium", app_title="Iceberg Consistency Model Part 2")


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
        create_spark_session,
        get_minio_config,
        get_polaris_config,
    )

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    polaris_config = get_polaris_config()
    minio_config = get_minio_config()
    return (
        create_duckdb_connection,
        create_iceberg_catalog,
        create_spark_session,
        mo,
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
            "iceberg_study", properties={"location": "s3://lakehouse/iceberg_study/"}
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
    spark = create_spark_session()
    return (spark,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # Apache Iceberg's Consistency Model - Part 2

    **Concurrency Control and Data Conflict Checks**

    This is an interactive companion to [Jack Vanlightly's article on Iceberg's consistency model - Part 2](https://jack-vanlightly.com/analyses/2024/8/5/apache-icebergs-consistency-model-part-2).

    ## What You'll Learn

    1. **Multi-writer scenarios** - How Iceberg handles concurrent writes
    2. **Compare-and-swap commits** - The atomic metadata file commit mechanism
    3. **Data conflict checks** - Validation checks for different operations
    4. **Conflict filters** - How partition-based filtering prevents conflicts
    5. **Operation types** - AppendFiles, OverwriteFiles, RowDelta, RewriteFiles
    6. **Isolation levels** - Snapshot Isolation vs Serializable
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 1: Iceberg Correctness in Multi-Writer Scenarios

    Iceberg supports multiple writers concurrently writing to the same table. The simplified model of a write follows these steps:

    ### The Iceberg Write Process

    ```
    1. Refresh Metadata     → Load current table state
    2. Read/Write Data      → Scan table and write new files
    3. Data Conflict Check  → Validate against new commits
    4. Write Metadata       → Create new metadata/snapshot
    5. Catalog Commit       → Atomic compare-and-swap (CAS)
    ```

    **Key Insight**: While an operation is in the reading and writing phase (step 2), another operation could commit a new snapshot. The operation will load this new snapshot in step 3 and perform conflict detection.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Scenario 1: Successful Concurrent Write

    Let's trace through a successful concurrent write scenario:

    ```
    Time ──────────────────────────────────────────────>

    Writer 1: [R]────[W]──────────[C]─────────────────
    Writer 2:        [R]────[W]───[Check]─[C]──────────

    Snapshot:  ──────snapshot-1───snapshot-2──────────

    [R] = Refresh Metadata    [W] = Read/Write
    [Check] = Conflict Check  [C] = Commit
    ```

    **What happens:**
    1. Both writers start based on snapshot-1
    2. Writer 1 commits quickly, creating snapshot-2
    3. Writer 2 refreshes metadata and sees snapshot-2
    4. Writer 2 performs conflict detection against snapshot-2 (passes)
    5. Writer 2 writes metadata based on snapshot-2
    6. Writer 2 commits successfully (CAS metadata-2 for metadata-3)
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Scenario 2: Commit Rejection and Retry

    Now let's see what happens when a commit fails:

    ```
    Time ──────────────────────────────────────────────>

    Writer 1:        [R]────[W]────────────[C]────────
    Writer 2: [R]────[W]──────[Check][C❌]──[R]─[C✓]──

    Snapshot:  ──────snapshot-1────────────────snapshot-2

    [C❌] = Commit rejected    [C✓] = Commit successful
    ```

    **What happens:**
    1. Writer 2 refreshes metadata before Writer 1 commits
    2. Writer 2's commit fails because its metadata is stale
    3. Writer 2 goes back and refreshes metadata again
    4. Writer 2 reruns the conflict check
    5. Writer 2 writes new metadata based on snapshot-2
    6. Writer 2 commits successfully

    **Key Point:** Writers can retry commits as long as there are no data conflicts!
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 2: Data Conflict Check + Metadata File Commit

    The foundation of Iceberg consistency lies in two mechanisms:

    ### 1. The Metadata File Commit (Compare-and-Swap)

    The catalog performs an **atomic compare-and-swap (CAS)** operation:

    ```
    Writer provides:
      • Current metadata location (what it THINKS is current)
      • New metadata location (where it wrote its new metadata)

    Catalog checks:
      If (provided_current == actual_current):
          ✓ Accept commit, update pointer to new metadata
      Else:
          ✗ Reject commit, writer must retry
    ```

    **Why this works:** The writer must base its new metadata on the CURRENT metadata. If another writer committed in between, the commit is rejected because the writer's base was stale.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### 2. The Data Conflict Check

    After a commit rejection, the writer refreshes its metadata and reruns validation checks. The data conflict check determines:

    **Definite File Conflicts:**
    - Two operations attempting to logically delete the same data file

    **Potential Data Conflicts:**
    - Based on row filters (data conflict filters)
    - Two operations modifying overlapping data

    ```
    No Conflict:                    Conflict Detected:
    ┌──────────┐   ┌──────────┐     ┌──────────┐   ┌──────────┐
    │ Writer 1 │   │ Writer 2 │     │ Writer 1 │   │ Writer 2 │
    │ ┌──────┐ │   │ ┌──────┐ │     │ ┌──────┐ │   │ ┌──────┐ │
    │ │File A│ │   │ │File B│ │     │ │File X│ │   │ │File X│ │
    │ └──────┘ │   │ └──────┘ │     │ └──────┘ │   │ └──────┘ │
    │ ┌──────┐ │   │ ┌──────┐ │     │ ┌──────┐ │   │   ⚠️     │
    │ │File C│ │   │ │File D│ │     │ │File Y│ │   │ Overlap! │
    │ └──────┘ │   │ └──────┘ │     │ └──────┘ │   │ └──────┘ │
    └──────────┘   └──────────┘     └──────────┘   └──────────┘

    Disjoint sets → No conflict    Overlapping → CONFLICT!
    ```
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 3: Data Conflict Filters

    **Data conflict filters** are unique to Apache Iceberg. They are row-based predicates that help reduce false conflicts.

    ### How They Work

    Without filters, validation fails if **ANY** data file was added in a post-scan snapshot. With filters, two operations touching **disjoint sets of data** can pass validation.

    ### Filter Application

    Manifest entry filtering works as follows:

    1. **Partition-based filtering:** If the filter contains partition spec columns
       - Manifest files of non-matching partitions are **skipped entirely**
       - Example: `color='blue'` filter skips manifests with `color='red'`

    2. **Statistics-based filtering:** Using column min/max statistics
       - If the predicate **cannot match** the upper/lower bounds, the entry is skipped
       - Example: `age > 30` skips files with `max_age <= 30`

    ```
    Partition: color='red'          Partition: color='blue'
    ┌────────────────────┐         ┌────────────────────┐
    │ Filter: color=blue │         │ Filter: color=blue │
    │ Result: SKIP       │         │ Result: CHECK      │
    └────────────────────┘         └────────────────────┘

    File stats: age=[20, 30]        File stats: age=[25, 35]
    ┌────────────────────┐         ┌────────────────────┐
    │ Filter: age > 30   │         │ Filter: age > 30   │
    │ Result: SKIP       │         │ Result: POTENTIAL  │
    └────────────────────┘         │ MATCH              │
                                   └────────────────────┘
    ```
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Conflict Filter Example: Partitioning

    Let's create a partitioned table to demonstrate conflict filtering:
    """)
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        DROP TABLE IF EXISTS lakehouse.iceberg_study.inventory;

        CREATE TABLE lakehouse.iceberg_study.inventory (
            id INTEGER,
            color VARCHAR,
            quantity INTEGER
        );

        INSERT INTO lakehouse.iceberg_study.inventory VALUES
            (1, 'red', 10),
            (2, 'red', 20),
            (3, 'blue', 15),
            (4, 'blue', 25),
            (5, 'green', 30);
        """,
        engine=con
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    Let's examine the table structure and partitions:
    """)
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT *
        FROM lakehouse.iceberg_study.inventory
        ORDER BY color, id
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT
            *
        FROM iceberg_snapshots('lakehouse.iceberg_study.inventory')
        ORDER BY sequence_number
        """,
        engine=con
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    **Scenario: Two writers working on different partitions**

    Let's create a properly partitioned table using the Iceberg catalog API and demonstrate concurrent writes:
    """)
    return


@app.cell
def _(mo, spark):
    # Set the current catalog to lakehouse
    spark.catalog.setCurrentCatalog("lakehouse")

    # Create namespace if not exists
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_study")

    # Drop if exists and create new partitioned table
    spark.sql("DROP TABLE IF EXISTS iceberg_study.partitioned_inventory")

    spark.sql("""
        CREATE TABLE iceberg_study.partitioned_inventory (
            id INT,
            color STRING,
            quantity INT
        ) USING ICEBERG
        PARTITIONED BY (color)
    """)

    # Insert initial data
    data = [
        (1, "red", 10),
        (2, "red", 20),
        (3, "blue", 15),
        (4, "blue", 25),
        (5, "green", 30),
    ]

    df = spark.createDataFrame(data, ["id", "color", "quantity"])
    df.writeTo("iceberg_study.partitioned_inventory").append()

    mo.md("""
    **Created partitioned table:**
    - Table: `iceberg_study.partitioned_inventory`
    - Partitioned by: `color`
    - Rows: 5
    - Partitions: red, blue, green
    - Using Spark with Iceberg REST catalog
    """)
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT *
        FROM lakehouse.iceberg_study.partitioned_inventory
        ORDER BY color, id
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT
            sequence_number,
            snapshot_id,
            manifest_list,
            timestamp_ms
        FROM iceberg_snapshots('lakehouse.iceberg_study.partitioned_inventory')
        ORDER BY sequence_number
        """,
        engine=con
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Concurrent Writers Simulation

    Now let's simulate two concurrent writers:

    ```python
    # Writer A: Updates color='red' items (IDs 1, 2)
    UPDATE partitioned_inventory
    SET quantity = quantity + 10
    WHERE color = 'red'

    # Writer B: Updates color='blue' items (IDs 3, 4)
    UPDATE partitioned_inventory
    SET quantity = quantity + 20
    WHERE color = 'blue'
    ```

    **With conflict filter `color='red'`:**
    - Writer A's filter: Only checks red partition → No conflict with blue partition
    - Writer B's filter: Only checks blue partition → No conflict with red partition
    - **Both can commit successfully!**

    **Without conflict filters:**
    - Writer A sees any data file was added → Conflict!
    - Writer B sees any data file was added → Conflict!
    - Only one can commit!

    Let's execute Writer A first:
    """)
    return


@app.cell
def _(mo, spark):
    # Writer A: Update red items with filter color='red'
    # Use Spark because DuckDB doesn't support UPDATE on partitioned tables
    spark.sql("""
        UPDATE iceberg_study.partitioned_inventory
        SET quantity = quantity + 10
        WHERE color = 'red'
    """)
    mo.md("✓ Writer A: Updated red items (quantity + 10)")
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT
            sequence_number,
            snapshot_id,
            manifest_list,
            timestamp_ms
        FROM iceberg_snapshots('lakehouse.iceberg_study.partitioned_inventory')
        ORDER BY sequence_number
        """,
        engine=con
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **Writer A committed successfully!**
    Notice:
    - New snapshot created with operation 'overwrite'
    - Files were added and deleted showing COW rewrite
    - Data files were rewritten for the red partition

    Now let's execute Writer B (updating blue items):
    """)
    return


@app.cell
def _(mo, spark):
    # Writer B: Update blue items with filter color='blue'
    # Use Spark because DuckDB doesn't support UPDATE on partitioned tables
    spark.sql("""
        UPDATE iceberg_study.partitioned_inventory
        SET quantity = quantity + 20
        WHERE color = 'blue'
    """)
    mo.md("✓ Writer B: Updated blue items (quantity + 20)")
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT
            sequence_number,
            snapshot_id,
            manifest_list,
            timestamp_ms
        FROM iceberg_snapshots('lakehouse.iceberg_study.partitioned_inventory')
        ORDER BY sequence_number
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT *
        FROM lakehouse.iceberg_study.partitioned_inventory
        ORDER BY color, id
        """,
        engine=con
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **Result: Both writers committed successfully!**

    Looking at the final data:
    - Red items (IDs 1, 2): quantity increased by 10 ✓
    - Blue items (IDs 3, 4): quantity increased by 20 ✓
    - Green item (ID 5): unchanged ✓

    **Why this worked:**
    1. Writer A used filter `color='red'` → only checked red partition files
    2. Writer B used filter `color='blue'` → only checked blue partition files
    3. Partitions are disjoint → no file-level conflicts
    4. Both commits succeeded in sequence

    **Key Takeaway:**
    When using proper conflict filters aligned with partitioning:
    - Writers targeting different partitions can commit concurrently
    - No unnecessary conflicts or retries
    - Better throughput in multi-writer scenarios

    **Without Conflict Filters:**
    If both writers had no filter (or used Serializable isolation):
    - Writer A's commit would succeed
    - Writer B would see new data files added → **CONFLICT!**
    - Writer B would need to retry or abort
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 4: Operations and Their Data Conflict Checks

    Iceberg has four operation types:

    | Operation | Type | Description |
    |-----------|------|-------------|
    | **APPEND** | APPEND | Only adds data files |
    | **OVERWRITE** | OVERWRITE | Row-level updates/deletes (COW or MOR) |
    | **REPLACE** | REPLACE | Compaction - replaces files with optimized versions |
    | **DELETE** | DELETE | Only deletes files |

    Let's explore the four key operations in detail.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Operation 1: AppendFiles

    **Type:** APPEND
    **Purpose:** Simple data file addition
    **Conflict Checks:** **NONE**

    AppendFiles is the simplest operation - it only adds new data files with no conflict checks.

    **Why no conflict checks?** Iceberg does not support primary keys natively. Duplicates are allowed. If two writers execute the same INSERT, both rows exist in the table.

    ```python
    # Pseudo-code for AppendFiles
    operation = table.newAppend()
    for file in new_data_files:
        operation.appendFile(file)
    operation.commit()  # No validation, just commit
    ```
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    **Demonstration: AppendFiles Operation**
    """)
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        DROP TABLE IF EXISTS lakehouse.iceberg_study.append_demo;

        CREATE TABLE lakehouse.iceberg_study.append_demo (
            id INTEGER,
            name VARCHAR,
            value INTEGER
        );

        INSERT INTO lakehouse.iceberg_study.append_demo VALUES
            (1, 'Alice', 100),
            (2, 'Bob', 200);
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        INSERT INTO lakehouse.iceberg_study.append_demo VALUES
            (3, 'Charlie', 300),
            (4, 'Diana', 400);
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT
            sequence_number,
            snapshot_id,
            manifest_list,
            timestamp_ms
        FROM iceberg_snapshots('lakehouse.iceberg_study.append_demo')
        ORDER BY sequence_number
        """,
        engine=con
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    **AppendFiles Results:**

    - Each INSERT creates a new snapshot
    - Operation type indicates AppendFiles
    - New files are added in each commit
    - New records are added

    Notice: No conflict checks were performed - appends always succeed!
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Operation 2: OverwriteFiles (Copy-on-Write)

    **Type:** OVERWRITE
    **Purpose:** Row-level updates/deletes using Copy-on-Write
    **Conflict Checks:** Yes - multiple validations

    **Methods called by compute engine:**
    - `addFile(DataFile file)` - Add new data files
    - `deleteFile(DataFile file)` - Logically delete data files
    - `commit()` - Validate and commit

    **Validation Checks:**

    1. **Fail Missing Delete Paths** (always enabled if others enabled)
       - Ensures all files in the deleted-set appear as DELETED in manifests
       - Prevents duplicate deletes across concurrent operations

    2. **No New Deletes Validation** (`validateNoConflictingDeletes()`)
       - Checks for delete files added after scan snapshot
       - Prevents COW conflicts with MOR operations

    3. **Added Data File Validation** (`validateNoConflictingData()`)
       - Checks for data files added after scan snapshot
       - Used for Serializable isolation
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    **Demonstration: OverwriteFiles (COW) Update**
    """)
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        DROP TABLE IF EXISTS lakehouse.iceberg_study.cow_overwrite_demo;

        CREATE TABLE lakehouse.iceberg_study.cow_overwrite_demo (
            id INTEGER,
            name VARCHAR,
            age INTEGER
        ) WITH (
            'write.update.mode' = 'copy-on-write',
            'write.delete.mode' = 'copy-on-write'
        );

        INSERT INTO lakehouse.iceberg_study.cow_overwrite_demo VALUES
            (1, 'Jack', 25),
            (2, 'Sarah', 30),
            (3, 'Mike', 35);
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM lakehouse.iceberg_study.cow_overwrite_demo ORDER BY id
        """,
        engine=con
    )
    return


@app.cell
def _(mo):
    mo.md("""
    **Now let's perform an UPDATE operation (triggers COW OverwriteFiles):**

    The UPDATE will:
    1. Read the current data
    2. Identify rows to update (WHERE clause)
    3. Rewrite the affected data files with new values
    4. Mark old files as DELETED, new files as ADDED
    """)
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        UPDATE lakehouse.iceberg_study.cow_overwrite_demo
        SET age = 26
        WHERE name = 'Jack';
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM lakehouse.iceberg_study.cow_overwrite_demo ORDER BY id
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT
            sequence_number,
            snapshot_id,
            manifest_list,
            timestamp_ms
        FROM iceberg_snapshots('lakehouse.iceberg_study.partitioned_inventory')
        ORDER BY sequence_number
        """,
        engine=con
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    **Analysis of COW Update:**

    - `operation='overwrite'` indicates OverwriteFiles
    - New snapshot shows: `added_files_count=1`, `deleted_files_count=1`
    - Old data file deleted, new file created with updated row
    - All rows were rewritten (even unchanged ones) because COW rewrites entire files

    **Conflict Scenario:** If two writers tried to update different rows in the **same data file**:
    - Writer A: UPDATE row 1 in file-X → deletes file-X, adds file-Y
    - Writer B: UPDATE row 2 in file-X → deletes file-X, adds file-Z
    - Writer A commits first → file-X deleted
    - Writer B's "Fail Missing Delete Paths" check fails → file-X already deleted!

    This prevents the conflict where both file-Y and file-Z would exist with different versions of row 1!
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Operation 3: RowDelta (Merge-on-Read)

    **Type:** OVERWRITE
    **Purpose:** Row-level updates/deletes using Merge-on-Read
    **Conflict Checks:** Yes - different validations than COW

    **Methods called by compute engine:**
    - `addRows(DataFile file)` - Add new data files
    - `addDeletes(DeleteFile deletes)` - Add delete files
    - `commit()` - Validate and commit

    **Validation Checks:**

    1. **Data Files Exist Validation** (always enabled)
       - Checks if data files referenced by delete files still exist
       - Prevents deleting already-deleted files

    2. **No New Delete Files Validation** (`validateNoConflictingDeleteFiles()`)
       - Checks for delete files added after scan snapshot
       - Prevents conflicts between multiple MOR operations

    3. **Added Data Files Validation** (`validateNoConflictingDataFiles()`)
       - Same as OverwriteFiles - checks for added data files
       - Used for Serializable isolation
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    **Demonstration: RowDelta (MOR) Update**
    """)
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        DROP TABLE IF EXISTS lakehouse.iceberg_study.mor_demo;

        CREATE TABLE lakehouse.iceberg_study.mor_demo (
            id INTEGER,
            name VARCHAR,
            age INTEGER
        ) WITH (
            'write.update.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read'
        );

        INSERT INTO lakehouse.iceberg_study.mor_demo VALUES
            (1, 'Jack', 25),
            (2, 'Sarah', 30),
            (3, 'Mike', 35);
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        UPDATE lakehouse.iceberg_study.mor_demo
        SET age = 26
        WHERE name = 'Jack';
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT
            sequence_number,
            snapshot_id,
            manifest_list,
            timestamp_ms
        FROM iceberg_snapshots('lakehouse.iceberg_study.mor_demo')
        ORDER BY sequence_number
        """,
        engine=con
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    **Analysis of MOR Update:**

    - Operation type is 'overwrite' (same as COW, but implementation differs)
    - MOR creates a **delete file** to mark old row as deleted
    - New data file contains only the updated row
    - Added delete files indicate MOR operation

    **Conflict Scenario:** If Writer A deletes file-X while Writer B creates a delete file for a row in file-X:
    - Writer B's delete file references file-X
    - Writer A commits first, deletes file-X
    - Writer B's "Data Files Exist" check fails → file-X doesn't exist!

    This prevents orphan delete files pointing to non-existent data!
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Operation 4: RewriteFiles (Compaction)

    **Type:** REPLACE
    **Purpose:** Optimize physical storage by rewriting files
    **Conflict Checks:** Yes - always enabled

    **What Compaction Does:**
    - Reads multiple small files
    - Rewrites them as fewer, larger, better-clustered files
    - Logically the same data, just optimized storage

    **Validation Checks:**

    1. **Fail Missing Delete Paths Validation** (always enabled)
       - Ensures files being replaced haven't been deleted
       - Prevents conflicts with concurrent deletes

    2. **No New Deletes Validation** (always enabled)
       - Checks for delete files added to files being replaced
       - Prevents conflicts with MOR operations

    **Note:** Unlike other operations, compaction doesn't have optional validations. Both checks are always enabled because compaction inherently replaces existing files.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 5: Isolation Levels in Apache Spark

    Different compute engines can configure which validation checks to enable. Apache Spark uses these configurations:

    ### Snapshot Isolation (SI)

    Spark enables:
    - ✓ `validateNoConflictingDeletes()` - No new deletes
    - ✗ `validateNoConflictingData()` - Skip added data check

    **Result:** Writers can see committed data from concurrent operations, but won't conflict on data additions.

    ### Serializable Isolation

    Spark enables:
    - ✓ `validateNoConflictingDeletes()` - No new deletes
    - ✓ `validateNoConflictingData()` - No new data files

    **Result:** Ensures serializable execution. If Operation A adds data matching Operation B's query filter, Operation B will fail and retry.

    **Why this matters:** Serializable prevents the "phantom read" anomaly where a query might see different results if re-executed after a concurrent commit.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Module 6: Practical Example - Concurrent Writers

    Let's demonstrate a realistic multi-writer scenario with conflict detection:
    """)
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        DROP TABLE IF EXISTS lakehouse.iceberg_study.concurrent_test;

        CREATE TABLE lakehouse.iceberg_study.concurrent_test (
            id INTEGER,
            category VARCHAR,
            value INTEGER
        );

        INSERT INTO lakehouse.iceberg_study.concurrent_test VALUES
            (1, 'A', 100),
            (2, 'A', 200),
            (3, 'B', 300),
            (4, 'B', 400);
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM lakehouse.iceberg_study.concurrent_test ORDER BY id
        """,
        engine=con
    )
    return


@app.cell
def _(mo):
    mo.md("""
    **Scenario: Two writers with different partition filters**

    ```
    Writer 1: UPDATE concurrent_test SET value = value + 10 WHERE category = 'A'
    Writer 2: UPDATE concurrent_test SET value = value + 20 WHERE category = 'B'
    ```

    **With good conflict filters:**
    - Writer 1 filter: `category='A'` → Only checks partition A
    - Writer 2 filter: `category='B'` → Only checks partition B
    - **Both can commit successfully!** (disjoint partition sets)

    **Without conflict filters or poor partitioning:**
    - Writer 1 sees any data was added → Conflict!
    - Writer 2 sees any data was added → Conflict!
    - One writer must retry or abort

    This demonstrates why **partitioning strategy matters** for multi-writer performance!
    """)
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        UPDATE lakehouse.iceberg_study.concurrent_test
        SET value = value + 10
        WHERE category = 'A';
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        UPDATE lakehouse.iceberg_study.concurrent_test
        SET value = value + 20
        WHERE category = 'B';
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM lakehouse.iceberg_study.concurrent_test ORDER BY id
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT
            sequence_number,
            snapshot_id,
            manifest_list,
            timestamp_ms
        FROM iceberg_snapshots('lakehouse.iceberg_study.concurrent_test')
        ORDER BY sequence_number
        """,
        engine=con
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Summary

    ### Key Concepts Covered

    1. **Multi-Writer Support**
       - Iceberg handles concurrent writes via compare-and-swap
       - Writers can retry commits if no data conflicts

    2. **Compare-and-Swap Commit**
       - Atomic metadata file replacement
       - Prevents lost updates from stale metadata

    3. **Data Conflict Checks**
       - Detect file-level and row-level conflicts
       - Different checks for different operation types

    4. **Data Conflict Filters**
       - Partition-based filtering
       - Statistics-based filtering (min/max bounds)
       - Reduces false conflicts in multi-writer scenarios

    5. **Operation Types**
       | Operation | Conflicts Checked? | Key Validations |
       |-----------|-------------------|-----------------|
       | **AppendFiles** | No | None |
       | **OverwriteFiles (COW)** | Yes | Delete paths, new deletes, added data |
       | **RowDelta (MOR)** | Yes | Data files exist, new delete files |
       | **RewriteFiles** | Yes | Delete paths, new deletes (always on) |

    6. **Isolation Levels (Spark)**
       - **Snapshot Isolation**: No added data check, allows concurrent appends
       - **Serializable**: Full conflict detection, prevents phantom reads

    ### Best Practices for Multi-Writer Topologies

    - **Use appropriate partitioning** - Align writers to partitions when possible
    - **Configure isolation level** - Use SI for better concurrency, Serializable for strict consistency
    - **Consider clustering** - Good data clustering improves conflict filter effectiveness
    - **Monitor retry rates** - High retry rates indicate partition conflicts

    ### Next Steps

    - Part 3 covers formal verification of Iceberg's consistency model
    - Explore how different query engines configure these validations
    - Benchmark your workload's conflict patterns
    """)
    return


if __name__ == "__main__":
    app.run()
