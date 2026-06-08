import logging

from airflow.exceptions import AirflowSkipException

from utils.s3 import list_partitions
from utils.spark import get_spark
from utils.paths import raw_key, staging_key, s3a

logger = logging.getLogger(__name__)

APP_NAME = "staging_high_volume_fhv"
LAKE_FOLDER = "high_volume_fhv"

ZONES_KEY = staging_key("reference/taxi_zone_lookup") + "/taxi_zone_lookup.parquet"


def stage_hvfhv(bucket: str) -> None:
    raw_partitions = set(list_partitions(bucket, raw_key(LAKE_FOLDER)))
    staging_partitions = set(list_partitions(bucket, staging_key(LAKE_FOLDER)))
    missing = sorted(raw_partitions - staging_partitions)

    if not missing:
        raise AirflowSkipException(f"No new partitions to stage for {LAKE_FOLDER}")

    logger.info("Staging %d partition(s) for %s: %s", len(missing), LAKE_FOLDER, missing)
    staging_path = s3a(bucket, staging_key(LAKE_FOLDER))

    spark = get_spark(APP_NAME)
    try:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        spark.read.parquet(s3a(bucket, ZONES_KEY)).createOrReplaceTempView("zones")

        for partition in missing:
            raw_path = s3a(bucket, raw_key(LAKE_FOLDER, partition))
            spark.read.parquet(raw_path).createOrReplaceTempView("raw")

            spark.sql(f"""
                SELECT /*+ BROADCAST(pu), BROADCAST(do) */
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
                    CAST(r.trip_miles AS DOUBLE) AS trip_distance_miles,
                    CAST(r.trip_time AS INT) AS trip_time_seconds,
                    pu.zone AS pickup_zone,
                    pu.borough AS pickup_borough,
                    do.zone AS dropoff_zone,
                    do.borough AS dropoff_borough,
                    CAST(r.base_passenger_fare AS DOUBLE) AS base_passenger_fare,
                    CAST(r.tolls AS DOUBLE) AS tolls_amount,
                    CAST(r.bcf AS DOUBLE) AS black_car_fund,
                    CAST(r.sales_tax AS DOUBLE) AS sales_tax,
                    CAST(r.congestion_surcharge AS DOUBLE) AS congestion_surcharge,
                    CAST(r.airport_fee AS DOUBLE) AS airport_fee,
                    CAST(r.tips AS DOUBLE) AS tip_amount,
                    CAST(r.driver_pay AS DOUBLE) AS driver_pay,
                    CAST(r.shared_request_flag AS STRING) AS shared_request_flag,
                    CAST(r.shared_match_flag AS STRING) AS shared_match_flag,
                    CAST(r.access_a_ride_flag AS STRING) AS access_a_ride_flag,
                    CAST(r.wav_request_flag AS STRING) AS wav_request_flag,
                    CAST(r.wav_match_flag AS STRING) AS wav_match_flag,
                    '{partition}' AS partition_date
                FROM raw r
                LEFT JOIN zones pu ON CAST(r.PULocationID AS INT) = pu.location_id
                LEFT JOIN zones do ON CAST(r.DOLocationID AS INT) = do.location_id
                WHERE CAST(r.pickup_datetime AS TIMESTAMP) IS NOT NULL
                  AND CAST(r.dropoff_datetime AS TIMESTAMP) IS NOT NULL
                  AND CAST(r.dropoff_datetime AS TIMESTAMP) > CAST(r.pickup_datetime AS TIMESTAMP)
                  AND CAST(r.PULocationID AS INT) IS NOT NULL
                  AND CAST(r.DOLocationID AS INT) IS NOT NULL
                  AND CAST(r.trip_miles AS DOUBLE) > 0
                  AND CAST(r.base_passenger_fare AS DOUBLE) > 0
            """).write.partitionBy("partition_date").mode("overwrite").parquet(staging_path)

            logger.info("Staged %s partition %s", LAKE_FOLDER, partition)
    finally:
        spark.stop()
