import logging
import os
import tempfile
import shutil
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, TimestampType, StringType
from airflow.exceptions import AirflowSkipException
from utils.s3 import upload_file, download_file, file_exists
from utils.spark import get_spark, get_parquet_output_path

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

APP_NAME = "staging_fhv"


def transform_fhv(input_path: str, output_dir: str) -> str:
    spark = get_spark(APP_NAME)

    df = spark.read.parquet(input_path)

    df = df.select(
        F.col("dispatching_base_num").cast(StringType()),
        F.col("pickup_datetime").cast(TimestampType()),
        F.col("dropOff_datetime").cast(TimestampType()).alias("dropoff_datetime"),
        F.col("PUlocationID").cast(IntegerType()).alias("pickup_location_id"),
        F.col("DOlocationID").cast(IntegerType()).alias("dropoff_location_id"),
        F.col("SR_Flag").cast(StringType()).alias("shared_ride_flag"),
        F.col("Affiliated_base_number").cast(StringType()).alias("affiliated_base_num"),
    )

    df = df.filter(
        F.col("pickup_datetime").isNotNull() &
        F.col("dropoff_datetime").isNotNull() &
        (F.col("dropoff_datetime") > F.col("pickup_datetime")) &
        F.col("dispatching_base_num").isNotNull() &
        F.col("pickup_location_id").isNotNull() &
        F.col("dropoff_location_id").isNotNull()
    )

    output_path = os.path.join(output_dir, os.path.basename(input_path))
    df.coalesce(1).write.mode("overwrite").parquet(output_path)

    final_path = get_parquet_output_path(output_path)
    logger.info(f"FHV staging saved to: {final_path}")
    return final_path


def stage_fhv(lake_folder: str, year: str, month: str, bucket: str):
    tmpfolder = tempfile.mkdtemp()
    try:
        file_name = f"fhv_tripdata_{year}-{int(month):02d}.parquet"
        raw_key = f"raw/{lake_folder}/partition_date={year}-{int(month):02d}-01/{file_name}"
        staging_key = f"staging/{lake_folder}/partition_date={year}-{int(month):02d}-01/{file_name}"

        if not file_exists(bucket=bucket, key=raw_key):
            raise AirflowSkipException(f"Raw file not found in MinIO: {raw_key}")

        input_path = os.path.join(tmpfolder, "input", file_name)
        os.makedirs(os.path.dirname(input_path), exist_ok=True)
        download_file(bucket=bucket, key=raw_key, filepath=input_path)

        output_dir = os.path.join(tmpfolder, "output")
        os.makedirs(output_dir, exist_ok=True)
        output_path = transform_fhv(input_path, output_dir)

        upload_file(filepath=output_path, bucket=bucket, key=staging_key)
        logger.info(f"Uploaded to {bucket}/{staging_key}")
    finally:
        shutil.rmtree(tmpfolder)
