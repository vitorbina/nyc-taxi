import logging

from utils.spark import get_spark
from utils.paths import final_key, s3a

logger = logging.getLogger(__name__)

APP_NAME = "final_zones_geo"


def compute_zones_geo(bucket: str) -> None:
    logger.info("Computing zones_geo from staging tables...")
    spark = get_spark(APP_NAME)
    try:
        spark.sql("""
            SELECT
                pickup_zone,
                COUNT(*)                        AS trip_count,
                ROUND(SUM(total_amount), 2)     AS total_revenue,
                ROUND(AVG(total_amount), 2)     AS avg_fare
            FROM final.revenue
            GROUP BY pickup_zone
        """).createOrReplaceTempView("_revenue_by_zone")

        spark.sql("""
            SELECT
                z.location_id,
                z.borough,
                z.zone,
                z.geometry_json,
                COALESCE(r.trip_count, 0)       AS trip_count,
                COALESCE(r.total_revenue, 0)    AS total_revenue,
                COALESCE(r.avg_fare, 0)         AS avg_fare
            FROM staging.taxi_zones_geo z
            LEFT JOIN _revenue_by_zone r ON z.zone = r.pickup_zone
        """).write.mode("overwrite").parquet(s3a(bucket, final_key("zones_geo")))

        row_count = spark.read.parquet(s3a(bucket, final_key("zones_geo"))).count()
        logger.info("Wrote %d rows to %s", row_count, s3a(bucket, final_key("zones_geo")))
    finally:
        spark.stop()
