import logging
from utils.spark import get_spark

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

APP_NAME = "final_revenue_by_zone"


def compute_revenue_by_zone(bucket: str):
    """
    Aggregates revenue metrics by pickup zone across all taxi types.
    app_rides is excluded — FHV data has no fare fields.
    """
    spark = get_spark(APP_NAME)

    spark.sql("""
        SELECT
            pickup_zone,
            pickup_borough,
            'yellow_taxi' AS taxi_type,
            COUNT(*)                     AS total_trips,
            ROUND(AVG(fare_amount), 2)   AS avg_fare,
            ROUND(AVG(tip_amount),  2)   AS avg_tip,
            ROUND(AVG(total_amount), 2)  AS avg_total
        FROM staging.yellow_taxi
        WHERE pickup_zone IS NOT NULL
        GROUP BY pickup_zone, pickup_borough

        UNION ALL

        SELECT
            pickup_zone,
            pickup_borough,
            'green_taxi',
            COUNT(*),
            ROUND(AVG(fare_amount), 2),
            ROUND(AVG(tip_amount),  2),
            ROUND(AVG(total_amount), 2)
        FROM staging.green_taxi
        WHERE pickup_zone IS NOT NULL
        GROUP BY pickup_zone, pickup_borough

        UNION ALL

        SELECT
            pickup_zone,
            pickup_borough,
            'high_volume_fhv',
            COUNT(*),
            ROUND(AVG(base_passenger_fare), 2),
            ROUND(AVG(tip_amount), 2),
            ROUND(AVG(base_passenger_fare + COALESCE(tip_amount, 0)), 2)
        FROM staging.high_volume_fhv
        WHERE pickup_zone IS NOT NULL
        GROUP BY pickup_zone, pickup_borough
    """).coalesce(1).write.mode("overwrite").parquet(f"s3a://{bucket}/final/revenue_by_zone")

    logger.info(f"revenue_by_zone written to s3a://{bucket}/final/revenue_by_zone")
    spark.stop()
