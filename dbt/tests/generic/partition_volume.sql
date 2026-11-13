{#
  Fails for partitions whose row count is far from the partitions before them:
  below min_ratio or above max_ratio times the average of the previous
  `lookback` partitions. Catches a truncated download or a file that came in
  much smaller (or larger) than usual. The first partition has nothing to
  compare with and is skipped. Comparing with recent months, not the whole
  history, keeps a long-term trend (e.g. yellow taxi shrinking since 2015)
  from failing it.
#}
{% test partition_volume(model, column_name, min_ratio=0.5, max_ratio=2.0, lookback=3) %}
WITH counts AS (
    SELECT {{ column_name }} AS partition_value, COUNT(*) AS row_count
    FROM {{ model }}
    GROUP BY 1
),

compared AS (
    SELECT
        partition_value,
        row_count,
        AVG(row_count) OVER (
            ORDER BY partition_value ROWS BETWEEN {{ lookback }} PRECEDING AND 1 PRECEDING
        ) AS previous_avg
    FROM counts
)

SELECT partition_value, row_count, ROUND(previous_avg) AS previous_avg
FROM compared
WHERE previous_avg IS NOT NULL
  AND (row_count < previous_avg * {{ min_ratio }} OR row_count > previous_avg * {{ max_ratio }})
{% endtest %}
