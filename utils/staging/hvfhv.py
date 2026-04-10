import logging
from airflow.exceptions import AirflowSkipException
from utils.s3 import file_exists
from utils.spark import get_spark

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

APP_NAME = "staging_hvfhv"


def stage_hvfhv(lake_folder: str, year: str, month: str, bucket: str):
    partition = f"partition_date={year}-{int(month):02d}-01"
    file_name = f"fhvhv_tripdata_{year}-{int(month):02d}.parquet"
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
            CASE r.hvfhs_license_num
                WHEN 'HV0002' THEN 'Juno'
                WHEN 'HV0003' THEN 'Uber'
                WHEN 'HV0004' THEN 'Via'
                WHEN 'HV0005' THEN 'Lyft'
            END AS company_name,
            CAST(r.dispatching_base_num AS STRING) AS dispatching_base_num,
            CAST(r.originating_base_num AS STRING) AS originating_base_num,
            CAST(r.request_datetime AS TIMESTAMP) AS request_datetime,
            CAST(r.on_scene_datetime AS TIMESTAMP) AS on_scene_datetime,
            CAST(r.pickup_datetime AS TIMESTAMP) AS pickup_datetime,
            CAST(r.dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
            (unix_timestamp(CAST(r.dropoff_datetime AS TIMESTAMP))
                - unix_timestamp(CAST(r.pickup_datetime AS TIMESTAMP))) / 60 AS trip_duration_minutes,
            CAST(r.trip_miles AS FLOAT) AS trip_distance_miles,
            CAST(r.trip_time AS INT) AS trip_time_seconds,
            pu.zone AS pickup_zone,
            pu.borough AS pickup_borough,
            do.zone AS dropoff_zone,
            do.borough AS dropoff_borough,
            CAST(r.base_passenger_fare AS FLOAT) AS base_passenger_fare,
            CAST(r.tolls AS FLOAT) AS tolls_amount,
            CAST(r.bcf AS FLOAT) AS black_car_fund,
            CAST(r.sales_tax AS FLOAT) AS sales_tax,
            CAST(r.congestion_surcharge AS FLOAT) AS congestion_surcharge,
            CAST(r.airport_fee AS FLOAT) AS airport_fee,
            CAST(r.tips AS FLOAT) AS tip_amount,
            CAST(r.driver_pay AS FLOAT) AS driver_pay,
            CAST(r.shared_request_flag AS STRING) AS shared_request_flag,
            CAST(r.shared_match_flag AS STRING) AS shared_match_flag,
            CAST(r.access_a_ride_flag AS STRING) AS access_a_ride_flag,
            CAST(r.wav_request_flag AS STRING) AS wav_request_flag,
            CAST(r.wav_match_flag AS STRING) AS wav_match_flag
        FROM raw r
        LEFT JOIN zones pu ON CAST(r.PULocationID AS INT) = pu.location_id
        LEFT JOIN zones do ON CAST(r.DOLocationID AS INT) = do.location_id
        WHERE CAST(r.pickup_datetime AS TIMESTAMP) IS NOT NULL
          AND CAST(r.dropoff_datetime AS TIMESTAMP) IS NOT NULL
          AND CAST(r.dropoff_datetime AS TIMESTAMP) > CAST(r.pickup_datetime AS TIMESTAMP)
          AND CAST(r.PULocationID AS INT) IS NOT NULL
          AND CAST(r.DOLocationID AS INT) IS NOT NULL
          AND CAST(r.trip_miles AS FLOAT) > 0
          AND CAST(r.base_passenger_fare AS FLOAT) > 0
    """).coalesce(1).write.mode("overwrite").parquet(f"s3a://{bucket}/{staging_key}")

    logger.info(f"HVFHV staging written to s3a://{bucket}/{staging_key}")
    spark.stop()
