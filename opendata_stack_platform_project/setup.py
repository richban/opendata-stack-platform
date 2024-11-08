from setuptools import find_packages, setup

setup(
    name="opendata_stack_platform",
    packages=find_packages(exclude=["opendata_stack_platform_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "boto3",
        "pandas",
        "matplotlib",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
