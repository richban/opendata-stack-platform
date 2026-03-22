import warnings

import dagster as dg

from dagster._utils.warnings import BetaWarning, PreviewWarning

import physical_risk_impact.defs

warnings.filterwarnings("ignore", category=PreviewWarning)
warnings.filterwarnings("ignore", category=BetaWarning)


defs = dg.Definitions.merge(
    dg.components.load_defs(physical_risk_impact.defs),
)
