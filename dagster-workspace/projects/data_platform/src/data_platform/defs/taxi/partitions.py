from dagster import MonthlyPartitionsDefinition

from data_platform.defs.taxi import constants

start_date = constants.START_DATE
end_date = constants.END_DATE

monthly_partition = MonthlyPartitionsDefinition(
    start_date=start_date, end_date=end_date
)
