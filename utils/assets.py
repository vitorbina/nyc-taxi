import os

from airflow.sdk import Asset

BUCKET = os.getenv("MINIO_BUCKET")

# Raw layer
raw_yellow_taxi = Asset(f"s3://{BUCKET}/raw/yellow_taxi")
raw_green_taxi = Asset(f"s3://{BUCKET}/raw/green_taxi")
raw_app_rides = Asset(f"s3://{BUCKET}/raw/app_rides")
raw_high_volume_fhv = Asset(f"s3://{BUCKET}/raw/high_volume_fhv")
raw_weather = Asset(f"s3://{BUCKET}/raw/weather")
raw_taxi_zones = Asset(f"s3://{BUCKET}/raw/reference/taxi_zone_lookup")

# Staging layer
staging_yellow_taxi = Asset(f"s3://{BUCKET}/staging/yellow_taxi")
staging_green_taxi = Asset(f"s3://{BUCKET}/staging/green_taxi")
staging_app_rides = Asset(f"s3://{BUCKET}/staging/app_rides")
staging_high_volume_fhv = Asset(f"s3://{BUCKET}/staging/high_volume_fhv")
staging_weather = Asset(f"s3://{BUCKET}/staging/weather")

raw_taxi_assets = {
    "yellow_taxi": raw_yellow_taxi,
    "green_taxi": raw_green_taxi,
    "app_rides": raw_app_rides,
    "high_volume_fhv": raw_high_volume_fhv,
}

staging_taxi_assets = {
    "yellow_taxi": staging_yellow_taxi,
    "green_taxi": staging_green_taxi,
    "app_rides": staging_app_rides,
    "high_volume_fhv": staging_high_volume_fhv,
}
