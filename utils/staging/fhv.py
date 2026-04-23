import logging
from airflow.exceptions import AirflowSkipException
from utils.s3 import file_exists
from utils.spark import get_spark

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

APP_NAME = "staging_app_rides"


def stage_fhv(lake_folder: str, year: str, month: str, bucket: str):
    partition = f"partition_date={year}-{int(month):02d}-01"
    file_name = f"fhv_tripdata_{year}-{int(month):02d}.parquet"
    raw_key = f"raw/{lake_folder}/{partition}/{file_name}"
    staging_key = f"staging/{lake_folder}/{partition}"
    zones_path = f"s3a://{bucket}/staging/reference/taxi_zone_lookup/taxi_zone_lookup.parquet"

    if not file_exists(bucket=bucket, key=raw_key):
        raise AirflowSkipException(f"Raw file not found in MinIO: {raw_key}")

    spark = get_spark(APP_NAME)
    spark.read.parquet(f"s3a://{bucket}/{raw_key}").createOrReplaceTempView("raw")
    spark.read.parquet(zones_path).createOrReplaceTempView("zones")

    spark.sql("""
        SELECT
            CAST(r.dispatching_base_num AS STRING) AS dispatching_base_num,
            CAST(r.Affiliated_base_number AS STRING) AS affiliated_base_num,
            CAST(r.pickup_datetime AS TIMESTAMP) AS pickup_datetime,
            CAST(r.dropOff_datetime AS TIMESTAMP) AS dropoff_datetime,
            (unix_timestamp(CAST(r.dropOff_datetime AS TIMESTAMP))
                - unix_timestamp(CAST(r.pickup_datetime AS TIMESTAMP))) / 60 AS trip_duration_minutes,
            CAST(r.SR_Flag AS STRING) AS shared_ride_flag,
            pu.zone AS pickup_zone,
            pu.borough AS pickup_borough,
            do.zone AS dropoff_zone,
            do.borough AS dropoff_borough
        FROM raw r
        LEFT JOIN zones pu ON CAST(r.PUlocationID AS INT) = pu.location_id
        LEFT JOIN zones do ON CAST(r.DOlocationID AS INT) = do.location_id
        WHERE CAST(r.pickup_datetime AS TIMESTAMP) IS NOT NULL
          AND CAST(r.dropOff_datetime AS TIMESTAMP) IS NOT NULL
          AND CAST(r.dropOff_datetime AS TIMESTAMP) > CAST(r.pickup_datetime AS TIMESTAMP)
          AND r.dispatching_base_num IS NOT NULL
          AND CAST(r.PUlocationID AS INT) IS NOT NULL
          AND CAST(r.DOlocationID AS INT) IS NOT NULL
    """).coalesce(1).write.mode("overwrite").parquet(f"s3a://{bucket}/{staging_key}")

    logger.info(f"FHV staging written to s3a://{bucket}/{staging_key}")
    spark.stop()
