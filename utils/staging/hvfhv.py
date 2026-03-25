import logging
import os
import tempfile
import shutil
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, TimestampType, StringType
from airflow.exceptions import AirflowSkipException
from utils.s3 import upload_file, download_file, file_exists
from utils.spark import get_spark, get_parquet_output_path

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

APP_NAME = "staging_hvfhv"


def transform_hvfhv(input_path: str, output_dir: str) -> str:
    spark = get_spark(APP_NAME)

    df = spark.read.parquet(input_path)

    df = df.select(
        F.col("hvfhs_license_num").cast(StringType()),
        F.col("dispatching_base_num").cast(StringType()),
        F.col("originating_base_num").cast(StringType()),
        F.col("request_datetime").cast(TimestampType()),
        F.col("on_scene_datetime").cast(TimestampType()),
        F.col("pickup_datetime").cast(TimestampType()),
        F.col("dropoff_datetime").cast(TimestampType()),
        F.col("PULocationID").cast(IntegerType()).alias("pickup_location_id"),
        F.col("DOLocationID").cast(IntegerType()).alias("dropoff_location_id"),
        F.col("trip_miles").cast(FloatType()).alias("trip_distance_miles"),
        F.col("trip_time").cast(IntegerType()).alias("trip_time_seconds"),
        F.col("base_passenger_fare").cast(FloatType()),
        F.col("tolls").cast(FloatType()).alias("tolls_amount"),
        F.col("bcf").cast(FloatType()).alias("black_car_fund"),
        F.col("sales_tax").cast(FloatType()),
        F.col("congestion_surcharge").cast(FloatType()),
        F.col("airport_fee").cast(FloatType()),
        F.col("tips").cast(FloatType()).alias("tip_amount"),
        F.col("driver_pay").cast(FloatType()),
        F.col("shared_request_flag").cast(StringType()),
        F.col("shared_match_flag").cast(StringType()),
        F.col("access_a_ride_flag").cast(StringType()),
        F.col("wav_request_flag").cast(StringType()),
        F.col("wav_match_flag").cast(StringType()),
    )

    df = df.filter(
        F.col("pickup_datetime").isNotNull() &
        F.col("dropoff_datetime").isNotNull() &
        (F.col("dropoff_datetime") > F.col("pickup_datetime")) &
        F.col("pickup_location_id").isNotNull() &
        F.col("dropoff_location_id").isNotNull() &
        (F.col("trip_distance_miles") > 0) &
        (F.col("base_passenger_fare") > 0)
    )

    output_path = os.path.join(output_dir, os.path.basename(input_path))
    df.coalesce(1).write.mode("overwrite").parquet(output_path)

    final_path = get_parquet_output_path(output_path)
    logger.info(f"HVFHV staging saved to: {final_path}")
    return final_path


def stage_hvfhv(lake_folder: str, year: str, month: str, bucket: str):
    tmpfolder = tempfile.mkdtemp()
    try:
        file_name = f"fhvhv_tripdata_{year}-{int(month):02d}.parquet"
        raw_key = f"raw/{lake_folder}/partition_date={year}-{int(month):02d}-01/{file_name}"
        staging_key = f"staging/{lake_folder}/partition_date={year}-{int(month):02d}-01/{file_name}"

        if not file_exists(bucket=bucket, key=raw_key):
            raise AirflowSkipException(f"Raw file not found in MinIO: {raw_key}")

        input_path = os.path.join(tmpfolder, "input", file_name)
        os.makedirs(os.path.dirname(input_path), exist_ok=True)
        download_file(bucket=bucket, key=raw_key, filepath=input_path)

        output_dir = os.path.join(tmpfolder, "output")
        os.makedirs(output_dir, exist_ok=True)
        output_path = transform_hvfhv(input_path, output_dir)

        upload_file(filepath=output_path, bucket=bucket, key=staging_key)
        logger.info(f"Uploaded to {bucket}/{staging_key}")
    finally:
        shutil.rmtree(tmpfolder)
