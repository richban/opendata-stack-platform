from dagster import (
    op,
    OpExecutionContext,
    Out,
    DynamicOutput,
    DynamicOut,
    In,
    AssetKey,
)
import polars as pl
import random
from typing import Dict, Any, List, Generator
from opendata_stack_platform.input_types import Asset


@op
def create_asset_from_row(row: Dict[str, Any]) -> Asset:
    """Convert each row into an Asset object."""
    return Asset(
        latitude=row["Latitude"],
        longitude=row["Longitude"],
        market_value=row["Value (USD)"],
    )


@op(ins={"df": In(asset_key=AssetKey(["portfolio_real_assets"]))}, out=DynamicOut())
def split_portfolio_to_rows(df: pl.DataFrame) -> Generator[DynamicOut, None, None]:
    """Yields each row into an Asset object of the portfolio as a dynamic output."""
    for idx, row in enumerate(df.iter_rows(named=True)):
        asset_input = create_asset_from_row(row)
        yield DynamicOutput(asset_input, mapping_key=f"row_{idx}")


@op(out=Out(io_manager_key="polars_csv_io_manager"))
def merge_and_analyze(dfs: List[pl.DataFrame]) -> pl.DataFrame:
    """Merge the results of each row into a single DataFrame."""
    return pl.concat([df for df in dfs])


@op(out=Out(io_manager_key="mem_io_manager"))
def calculate_climate_impact(input_asset: Asset) -> pl.DataFrame:
    """
    Generates projected annual average damage for a given asset based on warming levels.
    Randomly selects a warming level for each projection.
    """
    warming_levels = [1.5, 2.0, 2.5, 3.0]  # Available warming levels to choose from
    start_year = 2024
    end_year = 2050
    years = list(range(start_year, end_year + 1))
    asset_value = input_asset.market_value

    # Generate simulated damage values, max capped at 10% of the asset value, increasing with years
    damage_values = [
        min(asset_value * (0.05 + 0.001 * (year - start_year)), asset_value * 0.2)
        for year in years
    ]

    # Generate the DataFrame with random warming levels for each year
    climate_impact_df = pl.DataFrame(
        {
            "year": years,
            "warming_level": random.choice(
                warming_levels
            ),  # Random warming level selection
            "latitude": [input_asset.latitude] * len(years),
            "longitude": [input_asset.longitude] * len(years),
            "value": damage_values,
            "metric": ["average_annual_damage"] * len(years),
        }
    )

    return climate_impact_df


@op(out=Out(io_manager_key="mem_io_manager"))
def add_scenario_column(climate_impact_df: pl.DataFrame) -> pl.DataFrame:
    """
    Extends the climate impact DataFrame by adding a scenario column.
    Each row is randomly assigned an NGFS scenario.
    """
    # Define available NGFS scenarios
    scenarios = ["Net Zero 2050", "Delayed transition", "Current policies"]

    # Randomly assign scenarios to each row
    scenario_column = [
        random.choice(scenarios) for _ in range(climate_impact_df.height)
    ]

    # Add the scenario column to the DataFrame
    climate_impact_df = climate_impact_df.with_columns(
        pl.Series(name="scenario", values=scenario_column)
    )

    return climate_impact_df


@op(
    ins={"df": In(input_manager_key="polars_csv_io_manager")},
    out=DynamicOut(),
)
def input_split_portfolio_to_rows(
    context: OpExecutionContext, df: pl.LazyFrame
) -> Generator[DynamicOut, None, None]:
    """Yields each row into an Asset object of the portfolio as a dynamic output."""
    for idx, row in enumerate(df.collect().iter_rows(named=True)):
        asset_input = create_asset_from_row(row)
        yield DynamicOutput(asset_input, mapping_key=f"row_{idx}")
