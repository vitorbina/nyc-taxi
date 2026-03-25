import logging
import os
import tempfile
import shutil
from pyspark.sql import functions as F
from airflow.exceptions import AirflowSkipException
from utils.s3 import upload_file, download_file, file_exists
from utils.spark import get_spark, get_parquet_output_path, build_map_column

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

APP_NAME = "curated_yellow"

VENDOR_MAP = {
    1: "Creative Mobile Technologies, LLC",
    2: "Curb Mobility, LLC",
    6: "Myle Technologies Inc",
    7: "Helix",
}

RATE_CODE_MAP = {
    1: "Standard rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau or Westchester",
    5: "Negotiated fare",
    6: "Group ride",
    99: "Null/unknown",
}

PAYMENT_TYPE_MAP = {
    0: "Flex Fare trip",
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip",
}


def transform_yellow(input_path: str, zones_path: str, output_dir: str) -> str:
    spark = get_spark(APP_NAME)

    df = spark.read.parquet(input_path)
    zones = spark.read.parquet(zones_path)

    df = df.withColumn("vendor_name", build_map_column("vendor_id", VENDOR_MAP))
    df = df.withColumn("rate_code_name", build_map_column("rate_code_id", RATE_CODE_MAP))
    df = df.withColumn("payment_type_name", build_map_column("payment_type", PAYMENT_TYPE_MAP))

    df = df.withColumn(
        "trip_duration_minutes",
        (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60,
    )

    pickup_zones = zones.select(
        F.col("location_id").alias("pickup_location_id"),
        F.col("zone").alias("pickup_zone"),
        F.col("borough").alias("pickup_borough"),
    )
    dropoff_zones = zones.select(
        F.col("location_id").alias("dropoff_location_id"),
        F.col("zone").alias("dropoff_zone"),
        F.col("borough").alias("dropoff_borough"),
    )

    df = df.join(pickup_zones, on="pickup_location_id", how="left")
    df = df.join(dropoff_zones, on="dropoff_location_id", how="left")

    df = df.select(
        "pickup_datetime",
        "dropoff_datetime",
        "trip_duration_minutes",
        "vendor_name",
        "passenger_count",
        "trip_distance_miles",
        "rate_code_name",
        "store_and_fwd_flag",
        "pickup_zone",
        "pickup_borough",
        "dropoff_zone",
        "dropoff_borough",
        "payment_type_name",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "congestion_surcharge",
        "airport_fee",
        "total_amount",
    )

    output_path = os.path.join(output_dir, os.path.basename(input_path))
    df.coalesce(1).write.mode("overwrite").parquet(output_path)

    final_path = get_parquet_output_path(output_path)
    logger.info(f"Yellow curated saved to: {final_path}")
    return final_path


def curate_yellow(lake_folder: str, year: str, month: str, bucket: str):
    tmpfolder = tempfile.mkdtemp()
    try:
        file_name = f"yellow_tripdata_{year}-{int(month):02d}.parquet"
        staging_key = f"staging/{lake_folder}/partition_date={year}-{int(month):02d}-01/{file_name}"
        zones_key = "staging/reference/taxi_zone_lookup/taxi_zone_lookup.parquet"
        curated_key = f"curated/{lake_folder}/partition_date={year}-{int(month):02d}-01/{file_name}"

        if not file_exists(bucket=bucket, key=staging_key):
            raise AirflowSkipException(f"Staging file not found in MinIO: {staging_key}")

        input_path = os.path.join(tmpfolder, "input", file_name)
        os.makedirs(os.path.dirname(input_path), exist_ok=True)
        download_file(bucket=bucket, key=staging_key, filepath=input_path)

        zones_path = os.path.join(tmpfolder, "zones", "taxi_zone_lookup.parquet")
        os.makedirs(os.path.dirname(zones_path), exist_ok=True)
        download_file(bucket=bucket, key=zones_key, filepath=zones_path)

        output_dir = os.path.join(tmpfolder, "output")
        os.makedirs(output_dir, exist_ok=True)
        output_path = transform_yellow(input_path, zones_path, output_dir)

        upload_file(filepath=output_path, bucket=bucket, key=curated_key)
        logger.info(f"Uploaded to {bucket}/{curated_key}")
    finally:
        shutil.rmtree(tmpfolder)
