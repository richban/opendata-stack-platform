import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import logging
    import sys
    from pathlib import Path

    import duckdb
    import marimo as mo
    import sqlalchemy

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
    return (
        create_duckdb_connection,
        create_iceberg_catalog,
        duckdb,
        minio_config,
        mo,
        polaris_config,
        sqlalchemy,
    )


@app.cell
def _(create_iceberg_catalog):
    catalog = create_iceberg_catalog()
    return (catalog,)


@app.cell
def _(create_duckdb_connection):
    con = create_duckdb_connection()
    return (con,)


@app.cell
def _(sqlalchemy):
    postgres_engine = sqlalchemy.create_engine(
        "postgresql://polaris_user:polaris_password@localhost:5432/polaris_db"
    )
    return (postgres_engine,)


@app.cell
def _(mo, postgres_engine):
    _df = mo.sql(
        """
        SELECT * FROM polaris_schema.principal_authentication_data
        """,
        engine=postgres_engine,
    )


@app.cell(hide_code=True)
def _(sqlalchemy):
    sqlite_engine = sqlalchemy.create_engine("sqlite:///dq_results/dq_checks.db")
    return (sqlite_engine,)


@app.cell
def _(mo, sqlite_engine):
    _df = mo.sql(
        """
        select * from main.dq_results
        """,
        engine=sqlite_engine,
    )


@app.cell
def _(con):
    # Preview rows
    df = con.execute("""
        SELECT *
        FROM lakehouse.streamify.bronze_listen_events
        LIMIT 10
    """).df()

    df


@app.cell
def _(con):
    # Top artists by play count
    top_artists = con.execute("""
        SELECT
            artist,
            COUNT(*)               AS plays,
            COUNT(DISTINCT userId) AS unique_listeners
        FROM lakehouse.streamify.bronze_listen_events
        WHERE artist IS NOT NULL
        GROUP BY artist
        ORDER BY plays DESC
        LIMIT 20
    """).df()

    top_artists


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        SELECT * FROM iceberg_snapshots('lakehouse.streamify.bronze_listen_events');
        """,
        engine=con,
    )


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        select * from lakehouse.streamify.bronze_auth_events
        """,
        engine=con,
    )
    return _df


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        SELECT * FROM lakehouse.streamify.bronze_listen_events
        """,
        engine=con,
    )


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        SELECT * FROM lakehouse.streamify.bronze_page_view_events
        """,
        engine=con,
    )


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        SELECT * FROM lakehouse.streamify.silver_auth_events
        """,
        engine=con,
    )


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
