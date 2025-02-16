from setuptools import find_packages, setup

setup(
    name="opendata_stack_platform",
    packages=find_packages(exclude=["opendata_stack_platform_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dagster-embedded-elt",
        "dagster-polars",
        "dagster-aws",
        "dagster-duckdb",
        "boto3",
        "pandas",
        "matplotlib",
        "polars",
        "dbt-core",
        "dbt-duckdb",
        "duckdb",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
