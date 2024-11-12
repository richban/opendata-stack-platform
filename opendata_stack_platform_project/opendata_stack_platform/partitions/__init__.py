from dagster import MonthlyPartitionsDefinition
from opendata_stack_platform.assets import constants

start_date = constants.START_DATE
end_date = constants.END_DATE

monthly_partition = MonthlyPartitionsDefinition(
    start_date=start_date,
    end_date=end_date
)

