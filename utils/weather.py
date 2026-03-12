import os
import json
import logging
import requests
import tempfile
import shutil
from utils.s3 import upload_file

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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


def download_weather_data(date_str):
    tmpfolder = tempfile.mkdtemp()
    file_name = f"weather_nyc_{date_str}.json"
    local_path = os.path.join(tmpfolder, file_name)

    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={NYC_LAT}&longitude={NYC_LON}"
        f"&start_date={date_str}&end_date={date_str}"
        f"&hourly={','.join(HOURLY_VARIABLES)}"
        f"&timezone=America%2FNew_York"
    )

    logger.info(f"Fetching historical weather data from Open-Meteo for {date_str}...")

    response = requests.get(url)
    response.raise_for_status()

    data = response.json()

    with open(local_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    logger.info(f"Weather data saved to: {local_path}")
    return local_path


def upload_weather(local_path, date_str, bucket):
    file_name = os.path.basename(local_path)
    key = f"raw/weather/partition_date={date_str}/{file_name}"

    logger.info(f"Uploading to {bucket}/{key}")
    upload_file(filepath=local_path, bucket=bucket, key=key)

    tmpfolder = os.path.dirname(local_path)
    shutil.rmtree(tmpfolder)
    logger.info(f"Temporary folder {tmpfolder} removed.")


def ingest_weather_data(execution_date, bucket):
    date_str = execution_date.strftime("%Y-%m-%d")
    local_path = download_weather_data(date_str)
    upload_weather(local_path, date_str, bucket)