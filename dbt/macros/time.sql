{#
  TLC timestamps are New York wall-clock times with no zone. Comparing them as plain
  values goes wrong on the two DST nights: a trip from 01:30 EDT to 01:10 EST lasts
  40 minutes but looks negative. These macros read them as instants in New York, so
  durations and pickup/dropoff ordering match elapsed time (as Spark did with
  spark.sql.session.timeZone=America/New_York). The stored columns stay wall-clock.

  Converting every row through the time zone is slow (~1.6x on 240M rows), so the
  zone-aware path only runs for rows that can touch a DST switch: DST starts and ends
  on a Sunday within the first two weeks of March (2nd Sunday) and November (1st
  Sunday). Every other row gives the same answer as plain wall-clock arithmetic.

  The only remaining difference from Spark: the repeated hour when clocks fall back
  (01:00-01:59 happens twice). DuckDB reads it as the second one (EST), Java as the
  first (EDT). The source can't tell which, so either is a guess.
#}

{% macro ny_instant(ts) -%}
  timezone('America/New_York', {{ ts }})
{%- endmacro %}

{% macro near_dst_switch(ts) -%}
  (month({{ ts }}) IN (3, 11) AND dayofweek({{ ts }}) = 0 AND day({{ ts }}) <= 14)
{%- endmacro %}

{% macro elapsed_minutes(start_ts, end_ts) -%}
  CASE WHEN {{ near_dst_switch(start_ts) }} OR {{ near_dst_switch(end_ts) }}
       THEN date_diff('second', {{ ny_instant(start_ts) }}, {{ ny_instant(end_ts) }})
       ELSE date_diff('second', {{ start_ts }}, {{ end_ts }})
  END / 60
{%- endmacro %}

{% macro ny_is_after(later_ts, earlier_ts) -%}
  CASE WHEN {{ near_dst_switch(earlier_ts) }} OR {{ near_dst_switch(later_ts) }}
       THEN {{ ny_instant(later_ts) }} > {{ ny_instant(earlier_ts) }}
       ELSE {{ later_ts }} > {{ earlier_ts }}
  END
{%- endmacro %}
