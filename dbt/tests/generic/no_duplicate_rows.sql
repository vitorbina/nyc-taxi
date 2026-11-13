{#
  Model-level test: fails (one row, with the count) when the model has rows that
  are identical in every column. Raw TLC files have no primary key, so "the same
  record twice" is the closest check to a key.
#}
{% test no_duplicate_rows(model) %}
SELECT duplicate_rows
FROM (
    SELECT COUNT(*) - (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {{ model }})) AS duplicate_rows
    FROM {{ model }}
)
WHERE duplicate_rows > 0
{% endtest %}
