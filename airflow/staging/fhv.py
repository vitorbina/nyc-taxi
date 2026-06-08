import logging

from airflow.exceptions import AirflowSkipException

from utils.s3 import list_partitions
from utils.spark import get_spark
from utils.paths import raw_key, staging_key, s3a

logger = logging.getLogger(__name__)

APP_NAME = "staging_fhv"
LAKE_FOLDER = "app_rides"

ZONES_KEY = staging_key("reference/taxi_zone_lookup") + "/taxi_zone_lookup.parquet"


def stage_fhv(bucket: str) -> None:
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
                    '{partition}' AS partition_date
                FROM raw r
                LEFT JOIN zones pu ON CAST(r.PUlocationID AS INT) = pu.location_id
                LEFT JOIN zones do ON CAST(r.DOlocationID AS INT) = do.location_id
                WHERE CAST(r.pickup_datetime AS TIMESTAMP) IS NOT NULL
                  AND CAST(r.dropOff_datetime AS TIMESTAMP) IS NOT NULL
                  AND CAST(r.dropOff_datetime AS TIMESTAMP) > CAST(r.pickup_datetime AS TIMESTAMP)
                  AND r.dispatching_base_num IS NOT NULL
                  AND CAST(r.PUlocationID AS INT) IS NOT NULL
                  AND CAST(r.DOlocationID AS INT) IS NOT NULL
            """).write.partitionBy("partition_date").mode("overwrite").parquet(staging_path)

            logger.info("Staged %s partition %s", LAKE_FOLDER, partition)
    finally:
        spark.stop()
