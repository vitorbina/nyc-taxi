import os

PARTITION_COL = "partition_date"

STAGING_DATABASE = "staging"
RAW_DATABASE = "raw"
FINAL_DATABASE = "final"

DEFAULT_BUCKET = os.getenv("MINIO_BUCKET")

# Tables built by the dbt models in dbt/models/final, registered in the final database
FINAL_TABLES = [
    "trips",
    "revenue",
    "weather_impact",
    "zones_geo",
    "trips_hourly",
    "weather_daily",
]
