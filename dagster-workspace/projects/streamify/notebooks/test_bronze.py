import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")


@app.cell
def _():
    # .env.polaris is loaded by direnv (.envrc) — all POLARIS_* vars are already
    # in the environment. No load_dotenv needed.
    import os
    import duckdb
    from pyiceberg.catalog.rest import RestCatalog
    import marimo as mo
    import sqlalchemy

    POLARIS_CLIENT_ID     = os.getenv("POLARIS_CLIENT_ID")
    POLARIS_CLIENT_SECRET = os.getenv("POLARIS_CLIENT_SECRET")
    POLARIS_CATALOG       = os.getenv("POLARIS_CATALOG", "lakehouse")
    POLARIS_URI           = os.getenv("POLARIS_URI", "http://localhost:8181/api/catalog")

    MINIO_ENDPOINT        = os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000")
    MINIO_KEY             = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    MINIO_SECRET          = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

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
        sqlalchemy,
    )


@app.cell
def _(RestCatalog, os):

    catalog = RestCatalog(
        name=os.getenv("POLARIS_CATALOG", "lakehouse"),
        **{
            "uri":       os.getenv("POLARIS_URI", "http://localhost:8181/api/catalog"),
            "warehouse": os.getenv("POLARIS_CATALOG", "lakehouse"),
            "credential": f"{os.getenv('POLARIS_CLIENT_ID')}:{os.getenv('POLARIS_CLIENT_SECRET')}",
            "scope":     "PRINCIPAL_ROLE:ALL",
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


@app.cell
def _(sqlalchemy):
    postgres_engine = sqlalchemy.create_engine(
        "postgresql://polaris_user:polaris_password@localhost:5432/polaris_db"
    )
    return (postgres_engine,)


@app.cell
def _(mo, postgres_engine):
    _df = mo.sql(
        f"""
        SELECT * FROM polaris_schema.principal_authentication_data
        """,
        engine=postgres_engine
    )
    return


@app.cell
def _(sqlalchemy):
    sqlite_engine = sqlalchemy.create_engine("sqlite:///dq_results/dq_checks.db")
    return (sqlite_engine,)


@app.cell
def _(mo, sqlite_engine):
    _df = mo.sql(
        f"""
        select * from main.dq_results
        """,
        engine=sqlite_engine
    )
    return


@app.cell
def _(con):
    # Preview rows
    df = con.execute("""
        SELECT *
        FROM lakehouse.streamify.bronze_listen_events
        LIMIT 10
    """).df()

    df
    return


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
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM iceberg_snapshots('lakehouse.streamify.bronze_listen_events');
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM lakehouse.streamify.bronze_auth_events
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM lakehouse.streamify.bronze_listen_events
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM lakehouse.streamify.bronze_page_view_events
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM lakehouse.streamify.silver_auth_events
        """,
        engine=con
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
