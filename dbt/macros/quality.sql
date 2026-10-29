{#
  Trip-level quality rules shared by every final table.

  Staging keeps each TLC file as published (typed and translated). The source still
  contains records no BI consumer should see: a $863k yellow fare, trips of 276k
  miles, trips lasting days, and a few pickups dated outside the file's month. These
  rules drop them once (in the int_*_clean models), so trips, revenue and
  weather_impact all agree.
#}

{% macro clean_trip_rules(fare_column=none, has_distance=false) -%}
  -- pickup must fall inside the month of the file it came from
  pickup_datetime >= CAST(partition_date AS TIMESTAMP)
  AND pickup_datetime < CAST(partition_date AS TIMESTAMP) + INTERVAL 1 MONTH
  AND trip_duration_minutes BETWEEN {{ var('min_duration_minutes') }} AND {{ var('max_duration_minutes') }}
  {%- if has_distance %}
  AND trip_distance_miles > 0 AND trip_distance_miles <= {{ var('max_distance_miles') }}
  {%- endif %}
  {%- if fare_column %}
  AND {{ fare_column }} > 0 AND {{ fare_column }} <= {{ var('max_fare_usd') }}
  {%- endif %}
{%- endmacro %}
