import logging
import os
import tempfile
import shutil
from pyspark.sql import functions as F
from utils.s3 import upload_file, download_file
from utils.spark import get_spark, get_parquet_output_path

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

APP_NAME = "curated_weather"

WEATHER_CODE_MAP = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Rime fog",
    51: "Light drizzle",
    53: "Moderate drizzle",
    55: "Dense drizzle",
    61: "Slight rain",
    63: "Moderate rain",
    65: "Heavy rain",
    71: "Slight snow",
    73: "Moderate snow",
    75: "Heavy snow",
    77: "Snow grains",
    80: "Slight showers",
    81: "Moderate showers",
    82: "Violent showers",
    85: "Slight snow showers",
    86: "Heavy snow showers",
    95: "Thunderstorm",
    96: "Thunderstorm with hail",
    99: "Thunderstorm with heavy hail",
}


def _build_map_column(col_name: str, mapping: dict) -> F.Column:
    column = F.lit(None).cast("string")
    for code, label in mapping.items():
        column = F.when(F.col(col_name) == code, label).otherwise(column)
    return column


def transform_weather(input_path: str, output_dir: str, date_str: str) -> str:
    spark = get_spark(APP_NAME)

    df = spark.read.parquet(input_path)

    df = df.withColumn("weather_description", _build_map_column("weather_code", WEATHER_CODE_MAP))

    df = df.select(
        "datetime",
        "temperature_c",
        "apparent_temperature_c",
        "humidity_pct",
        "precipitation_mm",
        "wind_speed_kmh",
        "wind_direction_deg",
        "weather_description",
    )

    file_name = f"weather_nyc_{date_str}.parquet"
    output_path = os.path.join(output_dir, file_name)
    df.coalesce(1).write.mode("overwrite").parquet(output_path)

    final_path = get_parquet_output_path(output_path)
    logger.info(f"Weather curated saved to: {final_path}")
    return final_path


def curate_weather(date_str: str, bucket: str):
    tmpfolder = tempfile.mkdtemp()
    try:
        file_name = f"weather_nyc_{date_str}.parquet"
        staging_key = f"staging/weather/partition_date={date_str}/{file_name}"
        curated_key = f"curated/weather/partition_date={date_str}/{file_name}"

        input_path = os.path.join(tmpfolder, "input", file_name)
        os.makedirs(os.path.dirname(input_path), exist_ok=True)
        download_file(bucket=bucket, key=staging_key, filepath=input_path)

        output_dir = os.path.join(tmpfolder, "output")
        os.makedirs(output_dir, exist_ok=True)
        output_path = transform_weather(input_path, output_dir, date_str)

        upload_file(filepath=output_path, bucket=bucket, key=curated_key)
        logger.info(f"Uploaded to {bucket}/{curated_key}")
    finally:
        shutil.rmtree(tmpfolder)
