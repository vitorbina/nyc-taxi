import logging

from airflow.exceptions import AirflowSkipException

from utils.s3 import folder_exists
from utils.spark import get_spark
from utils.paths import raw_key, staging_key, s3a

logger = logging.getLogger(__name__)

APP_NAME = "staging_yellow_taxi"


ZONES_KEY = staging_key("reference/taxi_zone_lookup") + "/taxi_zone_lookup.parquet"


def stage_yellow(bucket: str) -> None:
    raw_prefix = raw_key("yellow_taxi")
    raw_path = s3a(bucket, raw_prefix)
    staging_path = s3a(bucket, staging_key("yellow_taxi"))

    if not folder_exists(bucket=bucket, prefix=raw_prefix):
        raise AirflowSkipException(f"No raw data found in MinIO: {raw_prefix}")

    logger.info("Staging yellow_taxi — raw: %s", raw_path)

    spark = get_spark(APP_NAME)
    try:
        spark.read.parquet(raw_path).createOrReplaceTempView("raw")
        spark.read.parquet(s3a(bucket, ZONES_KEY)).createOrReplaceTempView("zones")

        spark.sql("""
            SELECT
                CAST(r.VendorID AS INT) AS vendor_id,
                CASE CAST(r.VendorID AS INT)
                    WHEN 1 THEN 'Creative Mobile Technologies, LLC'
                    WHEN 2 THEN 'Curb Mobility, LLC'
                    WHEN 6 THEN 'Myle Technologies Inc'
                    WHEN 7 THEN 'Helix'
                END AS vendor_name,
                CAST(r.tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
                CAST(r.tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
                (unix_timestamp(CAST(r.tpep_dropoff_datetime AS TIMESTAMP))
                    - unix_timestamp(CAST(r.tpep_pickup_datetime AS TIMESTAMP))) / 60 AS trip_duration_minutes,
                CAST(r.passenger_count AS INT) AS passenger_count,
                CAST(r.trip_distance AS DOUBLE) AS trip_distance_miles,
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
                CAST(r.fare_amount AS DOUBLE) AS fare_amount,
                CAST(r.extra AS DOUBLE) AS extra,
                CAST(r.mta_tax AS DOUBLE) AS mta_tax,
                CAST(r.tip_amount AS DOUBLE) AS tip_amount,
                CAST(r.tolls_amount AS DOUBLE) AS tolls_amount,
                CAST(r.improvement_surcharge AS DOUBLE) AS improvement_surcharge,
                CAST(r.congestion_surcharge AS DOUBLE) AS congestion_surcharge,
                CAST(r.Airport_fee AS DOUBLE) AS airport_fee,
                CAST(r.total_amount AS DOUBLE) AS total_amount,
                r.partition_date
            FROM raw r
            LEFT JOIN zones pu ON CAST(r.PULocationID AS INT) = pu.location_id
            LEFT JOIN zones do ON CAST(r.DOLocationID AS INT) = do.location_id
            WHERE CAST(r.tpep_pickup_datetime AS TIMESTAMP) IS NOT NULL
              AND CAST(r.tpep_dropoff_datetime AS TIMESTAMP) IS NOT NULL
              AND CAST(r.tpep_dropoff_datetime AS TIMESTAMP) > CAST(r.tpep_pickup_datetime AS TIMESTAMP)
              AND CAST(r.PULocationID AS INT) IS NOT NULL
              AND CAST(r.DOLocationID AS INT) IS NOT NULL
              AND CAST(r.trip_distance AS DOUBLE) > 0
              AND CAST(r.fare_amount AS DOUBLE) > 0
              AND CAST(r.total_amount AS DOUBLE) > 0
              AND (r.passenger_count IS NULL OR CAST(r.passenger_count AS INT) > 0)
        """).repartition("partition_date").write.partitionBy("partition_date").mode("overwrite").parquet(staging_path)

        logger.info("Staged yellow_taxi to %s", staging_path)
    finally:
        spark.stop()
