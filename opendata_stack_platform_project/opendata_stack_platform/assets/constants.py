BUCKET = "datalake"
TAXI_ZONES_FILE_PATH = "raw/taxi_zones.csv"

TAXI_TRIPS_RAW_KEY_TEMPLATE = (
    "raw/{dataset_type}/{dataset_type}_tripdata_{partition}.parquet"
)

START_DATE = "2024-01-01"
END_DATE = "2024-12-01"
