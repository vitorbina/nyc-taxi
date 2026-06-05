import logging

from airflow.exceptions import AirflowSkipException

from utils.s3 import folder_exists
from utils.spark import get_spark
from utils.paths import raw_key, staging_key, s3a

logger = logging.getLogger(__name__)

APP_NAME = "staging_fhv"


ZONES_KEY = staging_key("reference/taxi_zone_lookup") + "/taxi_zone_lookup.parquet"


def stage_fhv(bucket: str) -> None:
    raw_prefix = raw_key("app_rides")
    raw_path = s3a(bucket, raw_prefix)
    staging_path = s3a(bucket, staging_key("app_rides"))

    if not folder_exists(bucket=bucket, prefix=raw_prefix):
        raise AirflowSkipException(f"No raw data found in MinIO: {raw_prefix}")

    logger.info("Staging app_rides — raw: %s", raw_path)

    spark = get_spark(APP_NAME)
    try:
        spark.read.parquet(raw_path).createOrReplaceTempView("raw")
        spark.read.parquet(s3a(bucket, ZONES_KEY)).createOrReplaceTempView("zones")

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
                do.borough AS dropoff_borough,
                r.partition_date
            FROM raw r
            LEFT JOIN zones pu ON CAST(r.PUlocationID AS INT) = pu.location_id
            LEFT JOIN zones do ON CAST(r.DOlocationID AS INT) = do.location_id
            WHERE CAST(r.pickup_datetime AS TIMESTAMP) IS NOT NULL
              AND CAST(r.dropOff_datetime AS TIMESTAMP) IS NOT NULL
              AND CAST(r.dropOff_datetime AS TIMESTAMP) > CAST(r.pickup_datetime AS TIMESTAMP)
              AND r.dispatching_base_num IS NOT NULL
              AND CAST(r.PUlocationID AS INT) IS NOT NULL
              AND CAST(r.DOlocationID AS INT) IS NOT NULL
        """).repartition("partition_date").write.partitionBy("partition_date").mode("overwrite").parquet(staging_path)

        logger.info("Staged app_rides to %s", staging_path)
    finally:
        spark.stop()
