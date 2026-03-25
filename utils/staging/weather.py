import logging
import json
import os
import tempfile
import shutil
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType, TimestampType
from airflow.exceptions import AirflowSkipException
from utils.s3 import upload_file, download_file, file_exists
from utils.spark import get_spark, get_parquet_output_path

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

APP_NAME = "staging_weather"


def transform_weather(input_path: str, output_dir: str, date_str: str) -> str:
    spark = get_spark(APP_NAME)

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

    df = spark.createDataFrame(rows)

    df = df.select(
        F.col("datetime").cast(TimestampType()),
        F.col("temperature_c").cast(FloatType()),
        F.col("apparent_temperature_c").cast(FloatType()),
        F.col("humidity_pct").cast(IntegerType()),
        F.col("precipitation_mm").cast(FloatType()),
        F.col("wind_speed_kmh").cast(FloatType()),
        F.col("wind_direction_deg").cast(IntegerType()),
        F.col("weather_code").cast(IntegerType()),
    )

    file_name = f"weather_nyc_{date_str}.parquet"
    output_path = os.path.join(output_dir, file_name)
    df.coalesce(1).write.mode("overwrite").parquet(output_path)

    final_path = get_parquet_output_path(output_path)
    logger.info(f"Weather staging saved to: {final_path}")
    return final_path


def stage_weather(date_str: str, bucket: str):
    tmpfolder = tempfile.mkdtemp()
    try:
        file_name = f"weather_nyc_{date_str}.json"
        raw_key = f"raw/weather/partition_date={date_str}/{file_name}"
        staging_key = f"staging/weather/partition_date={date_str}/weather_nyc_{date_str}.parquet"

        if not file_exists(bucket=bucket, key=raw_key):
            raise AirflowSkipException(f"Raw file not found in MinIO: {raw_key}")

        input_path = os.path.join(tmpfolder, "input", file_name)
        os.makedirs(os.path.dirname(input_path), exist_ok=True)
        download_file(bucket=bucket, key=raw_key, filepath=input_path)

        output_dir = os.path.join(tmpfolder, "output")
        os.makedirs(output_dir, exist_ok=True)
        output_path = transform_weather(input_path, output_dir, date_str)

        upload_file(filepath=output_path, bucket=bucket, key=staging_key)
        logger.info(f"Uploaded to {bucket}/{staging_key}")
    finally:
        shutil.rmtree(tmpfolder)
