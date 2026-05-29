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
                z.location_id,
                z.borough,
                z.zone,
                z.geometry_wkt,
                COUNT(r.pickup_zone)                    AS trip_count,
                ROUND(SUM(r.total_amount), 2)           AS total_revenue,
                ROUND(AVG(r.total_amount), 2)           AS avg_fare
            FROM staging.taxi_zones_geo z
            LEFT JOIN final.revenue r ON z.zone = r.pickup_zone
            GROUP BY z.location_id, z.borough, z.zone, z.geometry_wkt
        """).write.mode("overwrite").parquet(s3a(bucket, final_key("zones_geo")))

        row_count = spark.read.parquet(s3a(bucket, final_key("zones_geo"))).count()
        logger.info("Wrote %d rows to %s", row_count, s3a(bucket, final_key("zones_geo")))
    finally:
        spark.stop()
