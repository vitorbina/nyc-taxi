import logging
import os
import tempfile
import shutil
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, TimestampType, StringType
from utils.s3 import upload_file, download_file
from utils.spark import get_spark, get_parquet_output_path

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

APP_NAME = "staging_yellow_taxi"

def transform_yellow(input_path: str, output_dir: str) -> str:
    spark = get_spark(APP_NAME)

    df = spark.read.parquet(input_path)

    df = df.select(
        F.col("VendorID").cast(IntegerType()).alias("vendor_id"),
        F.col("tpep_pickup_datetime").cast(TimestampType()).alias("pickup_datetime"),
        F.col("tpep_dropoff_datetime").cast(TimestampType()).alias("dropoff_datetime"),
        F.col("passenger_count").cast(IntegerType()),
        F.col("trip_distance").cast(FloatType()).alias("trip_distance_miles"),
        F.col("RatecodeID").cast(IntegerType()).alias("rate_code_id"),
        F.col("store_and_fwd_flag").cast(StringType()),
        F.col("PULocationID").cast(IntegerType()).alias("pickup_location_id"),
        F.col("DOLocationID").cast(IntegerType()).alias("dropoff_location_id"),
        F.col("payment_type").cast(IntegerType()),
        F.col("fare_amount").cast(FloatType()),
        F.col("extra").cast(FloatType()),
        F.col("mta_tax").cast(FloatType()),
        F.col("tip_amount").cast(FloatType()),
        F.col("tolls_amount").cast(FloatType()),
        F.col("improvement_surcharge").cast(FloatType()),
        F.col("congestion_surcharge").cast(FloatType()),
        F.col("Airport_fee").cast(FloatType()).alias("airport_fee"),
        F.col("total_amount").cast(FloatType()),
    )

    df = df.filter(
        F.col("pickup_datetime").isNotNull() &
        F.col("dropoff_datetime").isNotNull() &
        (F.col("dropoff_datetime") > F.col("pickup_datetime")) &
        F.col("pickup_location_id").isNotNull() &
        F.col("dropoff_location_id").isNotNull() &
        (F.col("trip_distance_miles") > 0) &
        (F.col("fare_amount") > 0) &
        (F.col("total_amount") > 0) &
        (F.col("passenger_count").isNull() | (F.col("passenger_count") > 0))
    )

    output_path = os.path.join(output_dir, os.path.basename(input_path))
    df.coalesce(1).write.mode("overwrite").parquet(output_path)

    final_path = get_parquet_output_path(output_path)
    logger.info(f"Yellow staging saved to: {final_path}")
    return final_path


def stage_yellow(lake_folder: str, year: str, month: str, bucket: str):
    tmpfolder = tempfile.mkdtemp()
    try:
        file_name = f"yellow_tripdata_{year}-{int(month):02d}.parquet"
        raw_key = f"raw/{lake_folder}/partition_date={year}-{int(month):02d}-01/{file_name}"
        staging_key = f"staging/{lake_folder}/partition_date={year}-{int(month):02d}-01/{file_name}"

        input_path = os.path.join(tmpfolder, "input", file_name)
        os.makedirs(os.path.dirname(input_path), exist_ok=True)
        download_file(bucket=bucket, key=raw_key, filepath=input_path)

        output_dir = os.path.join(tmpfolder, "output")
        os.makedirs(output_dir, exist_ok=True)
        output_path = transform_yellow(input_path, output_dir)

        upload_file(filepath=output_path, bucket=bucket, key=staging_key)
        logger.info(f"Uploaded to {bucket}/{staging_key}")
    finally:
        shutil.rmtree(tmpfolder)
