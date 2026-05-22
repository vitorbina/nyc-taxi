import json
import logging
import os
import shutil
import tempfile
from datetime import datetime

import requests

from utils.s3 import upload_file
from utils.constants import PARTITION_COL

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

WEATHER_RAW_SCHEMA = (
    "`time` STRING, "
    "`temperature_2m` DOUBLE, "
    "`relative_humidity_2m` INT, "
    "`apparent_temperature` DOUBLE, "
    "`precipitation` DOUBLE, "
    "`wind_speed_10m` DOUBLE, "
    "`wind_direction_10m` INT, "
    "`weather_code` INT"
)


def _fetch_weather_data(date_str: str) -> dict:
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
    return response.json()


def _write_json_lines(payload: dict, local_path: str) -> None:
    hourly = payload["hourly"]
    keys = list(hourly.keys())
    rows = [dict(zip(keys, values)) for values in zip(*[hourly[k] for k in keys])]
    with open(local_path, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    logger.info("Weather data written as JSON Lines to %s (%d rows).", local_path, len(rows))


def _upload_weather_file(local_path: str, date_str: str, bucket: str) -> None:
    file_name = os.path.basename(local_path)
    key = f"raw/weather/{PARTITION_COL}={date_str}/{file_name}"
    logger.info("Uploading to %s/%s", bucket, key)
    upload_file(filepath=local_path, bucket=bucket, key=key)


def ingest_weather_data(execution_date: datetime, bucket: str) -> None:
    date_str = execution_date.strftime("%Y-%m-%d")
    file_name = f"weather_nyc_{date_str}.json"

    tmpdir = tempfile.mkdtemp()
    try:
        local_path = os.path.join(tmpdir, file_name)
        payload = _fetch_weather_data(date_str)
        _write_json_lines(payload, local_path)
        _upload_weather_file(local_path, date_str, bucket)
    finally:
        shutil.rmtree(tmpdir)
