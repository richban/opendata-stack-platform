from dagster import graph

from physical_risk_impact.defs.assets import source_portfolio_asset
from physical_risk_impact.defs.input_types import Asset
from physical_risk_impact.defs.ops import (
    add_scenario_column,
    calculate_climate_impact,
    input_split_portfolio_to_rows,
    merge_and_analyze,
    split_portfolio_to_rows,
)


@graph
def graph_calculation_climate_impact(input_asset: Asset):
    df_1 = calculate_climate_impact(input_asset)
    result = add_scenario_column(df_1)

    return result


@graph
def dynamic_graph_calculation_climate_impact():
    rows = split_portfolio_to_rows(source_portfolio_asset)
    results = rows.map(graph_calculation_climate_impact)
    return merge_and_analyze(results.collect())


@graph
def dynamic_sensor_graph_calculation_climate_impact():
    rows = input_split_portfolio_to_rows()
    results = rows.map(graph_calculation_climate_impact)
    return merge_and_analyze(results.collect())
