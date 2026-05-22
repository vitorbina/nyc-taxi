import logging

from utils.spark import get_spark
from utils.paths import final_key, s3a

logger = logging.getLogger(__name__)

APP_NAME = "final_trips"


def compute_trips(bucket: str) -> None:
    logger.info("Computing trips from staging tables...")
    spark = get_spark(APP_NAME)
    try:
        spark.sql("""
            SELECT
                pickup_datetime,
                dropoff_datetime,
                trip_duration_minutes,
                'yellow_taxi'       AS taxi_type,
                pickup_zone,
                pickup_borough,
                dropoff_zone,
                dropoff_borough,
                trip_distance_miles,
                total_amount        AS fare_amount,
                passenger_count
            FROM staging.yellow_taxi
            WHERE pickup_borough IS NOT NULL

            UNION ALL

            SELECT
                pickup_datetime,
                dropoff_datetime,
                trip_duration_minutes,
                'green_taxi'        AS taxi_type,
                pickup_zone,
                pickup_borough,
                dropoff_zone,
                dropoff_borough,
                trip_distance_miles,
                total_amount        AS fare_amount,
                passenger_count
            FROM staging.green_taxi
            WHERE pickup_borough IS NOT NULL

            UNION ALL

            SELECT
                pickup_datetime,
                dropoff_datetime,
                trip_duration_minutes,
                'app_rides'         AS taxi_type,
                pickup_zone,
                pickup_borough,
                dropoff_zone,
                dropoff_borough,
                NULL                AS trip_distance_miles,
                NULL                AS fare_amount,
                NULL                AS passenger_count
            FROM staging.app_rides
            WHERE pickup_borough IS NOT NULL

            UNION ALL

            SELECT
                pickup_datetime,
                dropoff_datetime,
                trip_duration_minutes,
                'high_volume_fhv'   AS taxi_type,
                pickup_zone,
                pickup_borough,
                dropoff_zone,
                dropoff_borough,
                trip_distance_miles,
                base_passenger_fare AS fare_amount,
                NULL                AS passenger_count
            FROM staging.high_volume_fhv
            WHERE pickup_borough IS NOT NULL
        """).write.mode("overwrite").parquet(s3a(bucket, final_key("trips")))

        row_count = spark.read.parquet(s3a(bucket, final_key("trips"))).count()
        logger.info("Wrote %d rows to %s", row_count, s3a(bucket, final_key("trips")))
    finally:
        spark.stop()
