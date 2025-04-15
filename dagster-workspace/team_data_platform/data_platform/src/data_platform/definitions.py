import dagster as dg

import data_platform.defs

defs = dg.Definitions.merge(
    dg.components.load_defs(data_platform.defs),
)
