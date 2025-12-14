import dagster as dg

import team_ops.defs

defs = dg.Definitions.merge(
    dg.components.load_defs(team_ops.defs),
)
