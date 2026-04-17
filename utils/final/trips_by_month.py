import logging
from utils.spark import get_spark

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

APP_NAME = "final_trips_by_month"


def compute_trips_by_month(bucket: str):
    spark = get_spark(APP_NAME)

    spark.sql("""
        SELECT
            year(pickup_datetime)  AS year,
            month(pickup_datetime) AS month,
            'yellow_taxi'          AS taxi_type,
            pickup_borough,
            COUNT(*)                        AS total_trips,
            ROUND(AVG(total_amount),  2)    AS avg_fare,
            ROUND(AVG(trip_distance_miles), 2) AS avg_distance_miles
        FROM staging.yellow_taxi
        WHERE pickup_borough IS NOT NULL
        GROUP BY year(pickup_datetime), month(pickup_datetime), pickup_borough

        UNION ALL

        SELECT
            year(pickup_datetime),
            month(pickup_datetime),
            'green_taxi',
            pickup_borough,
            COUNT(*),
            ROUND(AVG(total_amount), 2),
            ROUND(AVG(trip_distance_miles), 2)
        FROM staging.green_taxi
        WHERE pickup_borough IS NOT NULL
        GROUP BY year(pickup_datetime), month(pickup_datetime), pickup_borough

        UNION ALL

        SELECT
            year(pickup_datetime),
            month(pickup_datetime),
            'app_rides',
            pickup_borough,
            COUNT(*),
            NULL AS avg_fare,
            NULL AS avg_distance_miles
        FROM staging.app_rides
        WHERE pickup_borough IS NOT NULL
        GROUP BY year(pickup_datetime), month(pickup_datetime), pickup_borough

        UNION ALL

        SELECT
            year(pickup_datetime),
            month(pickup_datetime),
            'high_volume_fhv',
            pickup_borough,
            COUNT(*),
            ROUND(AVG(base_passenger_fare), 2),
            ROUND(AVG(trip_distance_miles), 2)
        FROM staging.high_volume_fhv
        WHERE pickup_borough IS NOT NULL
        GROUP BY year(pickup_datetime), month(pickup_datetime), pickup_borough
    """).coalesce(1).write.mode("overwrite").parquet(f"s3a://{bucket}/final/trips_by_month")

    logger.info(f"trips_by_month written to s3a://{bucket}/final/trips_by_month")
    spark.stop()
