from airflow.sdk import Asset

# Raw layer
raw_yellow_taxi = Asset("nyc-taxi/raw/yellow_taxi")
raw_green_taxi = Asset("nyc-taxi/raw/green_taxi")
raw_app_rides = Asset("nyc-taxi/raw/app_rides")
raw_high_volume_fhv = Asset("nyc-taxi/raw/high_volume_fhv")
raw_weather = Asset("nyc-taxi/raw/weather")
raw_taxi_zones = Asset("nyc-taxi/raw/taxi_zones")

# Staging layer
staging_yellow_taxi = Asset("nyc-taxi/staging/yellow_taxi")
staging_green_taxi = Asset("nyc-taxi/staging/green_taxi")
staging_app_rides = Asset("nyc-taxi/staging/app_rides")
staging_high_volume_fhv = Asset("nyc-taxi/staging/high_volume_fhv")
staging_weather = Asset("nyc-taxi/staging/weather")

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
