import logging
from airflow.exceptions import AirflowSkipException
from utils.s3 import file_exists
from utils.spark import get_spark

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

APP_NAME = "staging_green_taxi"


def stage_green(lake_folder: str, year: str, month: str, bucket: str):
    partition = f"partition_date={year}-{int(month):02d}-01"
    file_name = f"green_tripdata_{year}-{int(month):02d}.parquet"
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
            CAST(r.VendorID AS INT) AS vendor_id,
            CASE CAST(r.VendorID AS INT)
                WHEN 1 THEN 'Creative Mobile Technologies, LLC'
                WHEN 2 THEN 'Curb Mobility, LLC'
                WHEN 6 THEN 'Myle Technologies Inc'
                WHEN 7 THEN 'Helix'
            END AS vendor_name,
            CAST(r.lpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
            CAST(r.lpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
            (unix_timestamp(CAST(r.lpep_dropoff_datetime AS TIMESTAMP))
                - unix_timestamp(CAST(r.lpep_pickup_datetime AS TIMESTAMP))) / 60 AS trip_duration_minutes,
            CAST(r.passenger_count AS INT) AS passenger_count,
            CAST(r.trip_distance AS FLOAT) AS trip_distance_miles,
            CASE CAST(r.RatecodeID AS INT)
                WHEN 1 THEN 'Standard rate'
                WHEN 2 THEN 'JFK'
                WHEN 3 THEN 'Newark'
                WHEN 4 THEN 'Nassau or Westchester'
                WHEN 5 THEN 'Negotiated fare'
                WHEN 6 THEN 'Group ride'
                WHEN 99 THEN 'Null/unknown'
            END AS rate_code_name,
            CAST(r.store_and_fwd_flag AS STRING) AS store_and_fwd_flag,
            CASE CAST(r.trip_type AS INT)
                WHEN 1 THEN 'Street-hail'
                WHEN 2 THEN 'Dispatch'
            END AS trip_type_name,
            pu.zone AS pickup_zone,
            pu.borough AS pickup_borough,
            do.zone AS dropoff_zone,
            do.borough AS dropoff_borough,
            CASE CAST(r.payment_type AS INT)
                WHEN 0 THEN 'Flex Fare trip'
                WHEN 1 THEN 'Credit card'
                WHEN 2 THEN 'Cash'
                WHEN 3 THEN 'No charge'
                WHEN 4 THEN 'Dispute'
                WHEN 5 THEN 'Unknown'
                WHEN 6 THEN 'Voided trip'
            END AS payment_type_name,
            CAST(r.fare_amount AS FLOAT) AS fare_amount,
            CAST(r.extra AS FLOAT) AS extra,
            CAST(r.mta_tax AS FLOAT) AS mta_tax,
            CAST(r.tip_amount AS FLOAT) AS tip_amount,
            CAST(r.tolls_amount AS FLOAT) AS tolls_amount,
            CAST(r.improvement_surcharge AS FLOAT) AS improvement_surcharge,
            CAST(r.congestion_surcharge AS FLOAT) AS congestion_surcharge,
            CAST(r.total_amount AS FLOAT) AS total_amount
        FROM raw r
        LEFT JOIN zones pu ON CAST(r.PULocationID AS INT) = pu.location_id
        LEFT JOIN zones do ON CAST(r.DOLocationID AS INT) = do.location_id
        WHERE CAST(r.lpep_pickup_datetime AS TIMESTAMP) IS NOT NULL
          AND CAST(r.lpep_dropoff_datetime AS TIMESTAMP) IS NOT NULL
          AND CAST(r.lpep_dropoff_datetime AS TIMESTAMP) > CAST(r.lpep_pickup_datetime AS TIMESTAMP)
          AND CAST(r.PULocationID AS INT) IS NOT NULL
          AND CAST(r.DOLocationID AS INT) IS NOT NULL
          AND CAST(r.trip_distance AS FLOAT) > 0
          AND CAST(r.fare_amount AS FLOAT) > 0
          AND CAST(r.total_amount AS FLOAT) > 0
          AND (r.passenger_count IS NULL OR CAST(r.passenger_count AS INT) > 0)
    """).coalesce(1).write.mode("overwrite").parquet(f"s3a://{bucket}/{staging_key}")

    logger.info(f"Green staging written to s3a://{bucket}/{staging_key}")
    spark.stop()
