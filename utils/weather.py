import os
import json
import logging
import requests
import tempfile
import shutil

from utils.s3 import upload_file

logger = logging.getLogger(__name__)

NYC_LAT = 40.7128
NYC_LON = -74.0060

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "precipitation",
    "wind_speed_10m",
    "wind_direction_10m",
    "weather_code",
]


def _download_weather_data(date_str: str, local_path: str) -> None:
    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={NYC_LAT}&longitude={NYC_LON}"
        f"&start_date={date_str}&end_date={date_str}"
        f"&hourly={','.join(HOURLY_VARIABLES)}"
        f"&timezone=America%2FNew_York"
    )

    logger.info("Fetching weather data from Open-Meteo for %s...", date_str)

    response = requests.get(url)
    response.raise_for_status()

    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(response.json(), f, ensure_ascii=False, indent=4)

    logger.info("Weather data saved to: %s", local_path)


def _upload_weather_file(local_path: str, date_str: str, bucket: str) -> None:
    file_name = os.path.basename(local_path)
    key = f"raw/weather/partition_date={date_str}/{file_name}"
    logger.info("Uploading to %s/%s", bucket, key)
    upload_file(filepath=local_path, bucket=bucket, key=key)


def ingest_weather_data(execution_date, bucket: str) -> None:
    date_str = execution_date.strftime("%Y-%m-%d")
    file_name = f"weather_nyc_{date_str}.json"

    tmpdir = tempfile.mkdtemp()
    try:
        local_path = os.path.join(tmpdir, file_name)
        _download_weather_data(date_str, local_path)
        _upload_weather_file(local_path, date_str, bucket)
    finally:
        shutil.rmtree(tmpdir)
