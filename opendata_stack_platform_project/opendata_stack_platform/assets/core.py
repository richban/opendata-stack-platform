from dagster import AssetSpec, AssetKey, EnvVar, asset
import polars as pl

# Define the source asset and point to the file in your data lake
source_portfolio_asset = AssetSpec(
    key=AssetKey(["portfolio_real_assets"]),
    metadata={
        "aws_account": EnvVar("AWS_ACCOUNT").get_value(),
        "s3_location": "s3://datalake/portfolio_real_assets.csv",
    },
    description=("Contains portfolio of physical assets."),
    group_name="external_assets",
    # partition_key = MultiPartitionKey({"client_id": "client12345", "year": "2024"})
).with_io_manager_key("polars_csv_io_manager")


@asset
def derived_asset_from_source(portfolio_real_assets: pl.LazyFrame) -> pl.DataFrame:
    """
    Derived asset that takes the source asset (loaded by the I/O manager) and processes it.
    """
    filtered_df = portfolio_real_assets.filter(pl.col("Value (USD)") > 100)
    return filtered_df.collect()
