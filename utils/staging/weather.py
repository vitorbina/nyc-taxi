import logging
import json
import os
import tempfile
import shutil
from airflow.exceptions import AirflowSkipException
from utils.s3 import download_file, file_exists
from utils.spark import get_spark

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

APP_NAME = "staging_weather"


def stage_weather(date_str: str, bucket: str):
    file_name = f"weather_nyc_{date_str}.json"
    raw_key = f"raw/weather/partition_date={date_str}/{file_name}"
    staging_key = f"staging/weather/partition_date={date_str}/weather_nyc_{date_str}.parquet"

    if not file_exists(bucket=bucket, key=raw_key):
        raise AirflowSkipException(f"Raw file not found in MinIO: {raw_key}")

    tmpfolder = tempfile.mkdtemp()
    try:
        input_path = os.path.join(tmpfolder, file_name)
        download_file(bucket=bucket, key=raw_key, filepath=input_path)

        with open(input_path, "r") as f:
            data = json.load(f)

        hourly = data["hourly"]
        rows = [
            {
                "datetime": hourly["time"][i],
                "temperature_c": hourly["temperature_2m"][i],
                "apparent_temperature_c": hourly["apparent_temperature"][i],
                "humidity_pct": hourly["relative_humidity_2m"][i],
                "precipitation_mm": hourly["precipitation"][i],
                "wind_speed_kmh": hourly["wind_speed_10m"][i],
                "wind_direction_deg": hourly["wind_direction_10m"][i],
                "weather_code": hourly["weather_code"][i],
            }
            for i in range(len(hourly["time"]))
        ]

        spark = get_spark(APP_NAME)
        spark.createDataFrame(rows).createOrReplaceTempView("raw")

        spark.sql("""
            SELECT
                CAST(datetime AS TIMESTAMP) AS datetime,
                CAST(temperature_c AS FLOAT) AS temperature_c,
                CAST(apparent_temperature_c AS FLOAT) AS apparent_temperature_c,
                CAST(humidity_pct AS INT) AS humidity_pct,
                CAST(precipitation_mm AS FLOAT) AS precipitation_mm,
                CAST(wind_speed_kmh AS FLOAT) AS wind_speed_kmh,
                CAST(wind_direction_deg AS INT) AS wind_direction_deg,
                CAST(weather_code AS INT) AS weather_code,
                CASE CAST(weather_code AS INT)
                    WHEN 0  THEN 'Clear sky'
                    WHEN 1  THEN 'Mainly clear'
                    WHEN 2  THEN 'Partly cloudy'
                    WHEN 3  THEN 'Overcast'
                    WHEN 45 THEN 'Fog'
                    WHEN 48 THEN 'Rime fog'
                    WHEN 51 THEN 'Light drizzle'
                    WHEN 53 THEN 'Moderate drizzle'
                    WHEN 55 THEN 'Dense drizzle'
                    WHEN 61 THEN 'Slight rain'
                    WHEN 63 THEN 'Moderate rain'
                    WHEN 65 THEN 'Heavy rain'
                    WHEN 71 THEN 'Slight snow'
                    WHEN 73 THEN 'Moderate snow'
                    WHEN 75 THEN 'Heavy snow'
                    WHEN 77 THEN 'Snow grains'
                    WHEN 80 THEN 'Slight showers'
                    WHEN 81 THEN 'Moderate showers'
                    WHEN 82 THEN 'Violent showers'
                    WHEN 85 THEN 'Slight snow showers'
                    WHEN 86 THEN 'Heavy snow showers'
                    WHEN 95 THEN 'Thunderstorm'
                    WHEN 96 THEN 'Thunderstorm with hail'
                    WHEN 99 THEN 'Thunderstorm with heavy hail'
                END AS weather_description
            FROM raw
        """).coalesce(1).write.mode("overwrite").parquet(f"s3a://{bucket}/{staging_key}")

        logger.info(f"Weather staging written to s3a://{bucket}/{staging_key}")
    finally:
        spark.stop()
        shutil.rmtree(tmpfolder)
