import logging

from airflow.exceptions import AirflowSkipException

from utils.s3 import file_exists
from utils.spark import get_spark
from utils.paths import raw_key, staging_key, s3a

logger = logging.getLogger(__name__)

APP_NAME = "staging_fhv"


ZONES_KEY = staging_key("reference/taxi_zone_lookup") + "/taxi_zone_lookup.parquet"


def stage_fhv(lake_folder: str, year: str, month: str, bucket: str) -> None:
    partition = f"{year}-{int(month):02d}-01"
    file_name = f"fhv_tripdata_{year}-{int(month):02d}.parquet"
    rk = raw_key(lake_folder, partition, file_name)
    sk = staging_key(lake_folder, partition)

    if not file_exists(bucket=bucket, key=rk):
        raise AirflowSkipException(f"Raw file not found in MinIO: {rk}")

    logger.info("Staging %s for %s-%s — raw: %s", lake_folder, year, month, s3a(bucket, rk))

    spark = get_spark(f"{APP_NAME}_{year}-{int(month):02d}")
    try:
        spark.read.parquet(s3a(bucket, rk)).createOrReplaceTempView("raw")
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
        """).write.mode("overwrite").parquet(s3a(bucket, sk))

        row_count = spark.read.parquet(s3a(bucket, sk)).count()
        logger.info("Wrote %d rows to %s", row_count, s3a(bucket, sk))
    finally:
        spark.stop()
