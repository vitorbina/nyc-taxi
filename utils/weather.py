import os
import json
import logging
import requests
import tempfile
import shutil
from utils.s3 import upload_file

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def download_weather_data(api_key, date_str):
    tmpfolder = tempfile.mkdtemp()
    file_name = f"weather_nyc_{date_str}.json"
    local_path = os.path.join(tmpfolder, file_name)

    url = f"http://api.openweathermap.org/data/2.5/weather?q=New York,US&appid={api_key}&units=metric"

    logger.info("Fetching weather data from OpenWeatherMap API...")

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
    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        raise ValueError("OPENWEATHER_API_KEY not found.")

    date_str = execution_date.strftime("%Y-%m-%d")
    local_path = download_weather_data(api_key, date_str)
    upload_weather(local_path, date_str, bucket)