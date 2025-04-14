from typing import Union

from dagster import (
    DagsterTypeLoaderContext,
    dagster_type_loader,
    usable_as_dagster_type,
)


@dagster_type_loader(
    config_schema={
        "latitude": float,
        "longitude": float,
        "market_value": float,
    }
)
def asset_loader(
    _context: DagsterTypeLoaderContext, config: dict[str, Union[int, float, str]]
) -> "Asset":
    """
    Loader function for the Asset class.
    """
    return Asset(
        latitude=config["latitude"],
        longitude=config["longitude"],
        market_value=config["market_value"],
    )


@usable_as_dagster_type(loader=asset_loader)
class Asset:
    def __init__(
        self,
        latitude: float,
        longitude: float,
        market_value: float,
    ):
        self.latitude = latitude
        self.longitude = longitude
        self.market_value = market_value
