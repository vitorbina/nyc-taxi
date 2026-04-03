"""
Q4 - Weather Impact on Taxi Demand

Joins yellow taxi trips with hourly weather data to compare
trip volume, fares, and tips across dry, rainy, and heavy rain/snow conditions.
"""

from queries.spark_minio import get_spark, curated

spark = get_spark("weather_demand")

spark.read.parquet(curated("yellow_taxi")).createOrReplaceTempView("yellow")
spark.read.parquet(curated("weather")).createOrReplaceTempView("weather")

result = spark.sql("""
    WITH taxi_hourly AS (
        SELECT
            to_date(pickup_datetime)  AS date,
            hour(pickup_datetime)     AS hour,
            COUNT(*)                  AS trip_count,
            ROUND(AVG(total_amount), 2)       AS avg_fare,
            ROUND(AVG(tip_amount), 2)         AS avg_tip,
            ROUND(AVG(trip_distance_miles), 2) AS avg_distance_miles
        FROM yellow
        GROUP BY date, hour
    ),
    weather_hourly AS (
        SELECT
            to_date(datetime)   AS date,
            hour(datetime)      AS hour,
            temperature_c,
            precipitation_mm,
            weather_description,
            wind_speed_kmh
        FROM weather
    ),
    joined AS (
        SELECT
            t.*,
            w.temperature_c,
            w.precipitation_mm,
            w.weather_description,
            w.wind_speed_kmh,
            CASE
                WHEN w.precipitation_mm >= 5 THEN 'Heavy Rain/Snow'
                WHEN w.precipitation_mm  > 0 THEN 'Light Rain/Drizzle'
                ELSE 'Dry'
            END AS weather_category
        FROM taxi_hourly t
        INNER JOIN weather_hourly w ON t.date = w.date AND t.hour = w.hour
    )
    SELECT
        weather_category,
        COUNT(*)                            AS hourly_slots,
        ROUND(AVG(trip_count), 1)           AS avg_trips_per_hour,
        ROUND(AVG(avg_fare), 2)             AS avg_fare,
        ROUND(AVG(avg_tip), 2)              AS avg_tip,
        ROUND(AVG(temperature_c), 1)        AS avg_temp_c,
        ROUND(AVG(precipitation_mm), 2)     AS avg_precipitation_mm
    FROM joined
    GROUP BY weather_category
    ORDER BY weather_category
""")

result.show(truncate=False)
