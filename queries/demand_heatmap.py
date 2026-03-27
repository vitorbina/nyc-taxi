"""
Q3 - Demand Heatmap: Hour x Day of Week

Shows when yellow taxi demand is highest throughout the week,
useful for identifying rush hours and weekend patterns.
"""

from queries.spark_minio import get_spark, curated

spark = get_spark("demand_heatmap")

spark.read.parquet(curated("yellow_taxi")).createOrReplaceTempView("yellow")

result = spark.sql("""
    SELECT
        dayofweek(pickup_datetime)              AS day_of_week,
        date_format(pickup_datetime, 'EEEE')    AS day_name,
        hour(pickup_datetime)                   AS hour,
        COUNT(*)                                AS trip_count,
        ROUND(AVG(trip_duration_minutes), 2)    AS avg_duration_min,
        ROUND(AVG(total_amount), 2)             AS avg_fare
    FROM yellow
    GROUP BY day_of_week, day_name, hour
    ORDER BY day_of_week, hour
""")

result.show(200, truncate=False)
