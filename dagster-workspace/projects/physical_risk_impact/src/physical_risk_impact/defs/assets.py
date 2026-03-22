import polars as pl

from dagster import (
    AssetKey,
    AssetSpec,
    EnvVar,
    asset,
)

# Define the source asset and point to the file in your data lake
source_portfolio_asset = AssetSpec(
    key=AssetKey(["portfolio_real_assets"]),
    metadata={
        "aws_account": EnvVar("AWS_ACCOUNT").get_value(),
        "s3_location": "s3://datalake/portfolio_real_assets.csv",
    },
    description=("Contains portfolio of physical assets."),
    group_name="external_assets",
).with_io_manager_key("polars_csv_io_manager")


# Define constants
MIN_VALUE_USD = 100


@asset(group_name="core")
def derived_asset_from_source(portfolio_real_assets: pl.LazyFrame) -> pl.DataFrame:
    """Process the source asset by filtering based on minimum value threshold.

    Takes the portfolio real assets DataFrame and filters out entries below the minimum
    value threshold defined by MIN_VALUE_USD constant.

    Args:
        portfolio_real_assets: A LazyFrame containing portfolio asset data with a
            'Value (USD)' column.

    Returns:
        pl.DataFrame: A filtered DataFrame containing only assets above the minimum
            value threshold.
    """
    filtered_df = portfolio_real_assets.filter(pl.col("Value (USD)") > MIN_VALUE_USD)
    return filtered_df.collect()
