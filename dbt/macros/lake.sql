{#
  `lake` materialization: stream a model's SELECT straight into Parquet on MinIO.

  dbt-duckdb's built-in `external` first builds the whole result as a temp table in
  DuckDB and only then writes it out. For trip tables (tens to hundreds of millions
  of rows) that is ~4x slower and holds everything in memory/spill. A plain
  COPY (...) TO streams instead. After writing, a view over the files is created so
  downstream models and tests can `ref()` it within the same run.

  Config:
    location      s3:// path. A directory when partitioned, a .parquet file otherwise.
    partition_by  optional column; written as Hive-style folders (col=value/).
#}

{% macro lake_location(subpath) -%}
  {{ var('output_lake', 's3://' ~ env_var('MINIO_BUCKET')) }}/{{ subpath }}
{%- endmacro %}

{% macro lake_read(location, partition_by) -%}
  {%- if partition_by -%}
    read_parquet('{{ location }}/*/*.parquet', hive_partitioning=true)
  {%- else -%}
    read_parquet('{{ location }}')
  {%- endif -%}
{%- endmacro %}

{% materialization lake, adapter='duckdb' %}
  {%- set location = config.require('location') -%}
  {%- set partition_by = config.get('partition_by') -%}
  {%- set target_relation = this.incorporate(type='view') -%}

  {{ run_hooks(pre_hooks) }}

  {% call statement('main') -%}
    COPY ({{ sql }}) TO '{{ location }}' (
      FORMAT parquet
      {%- if partition_by %}, PARTITION_BY ({{ partition_by }}), OVERWRITE_OR_IGNORE{% endif %}
    )
  {%- endcall %}

  {% call statement('create_view') -%}
    CREATE OR REPLACE VIEW {{ target_relation }} AS SELECT * FROM {{ lake_read(location, partition_by) }}
  {%- endcall %}
  {# make the view visible to models running on other threads #}
  {% do adapter.commit() %}

  {{ run_hooks(post_hooks) }}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}

{#
  Each dbt invocation starts with an empty in-memory DuckDB. When only part of the
  project runs (`dbt run -s stg_yellow_taxi`, or one model per Airflow task), the
  upstream `lake` models it refs have no view yet. This on-run-start hook creates
  those views, reading the upstream files back from MinIO.
#}
{% macro register_upstream_lake_models() %}
  {% if execute %}
    {% set done = [] %}
    {% for node_id in selected_resources %}
      {% set node = graph.nodes.get(node_id) %}
      {% if node %}
        {% for upstream_id in node.depends_on.nodes %}
          {% set upstream = graph.nodes.get(upstream_id) %}
          {% if upstream and upstream_id not in selected_resources and upstream_id not in done
                and upstream.config.materialized == 'lake' %}
            {% do done.append(upstream_id) %}
            {% set rel = api.Relation.create(database=upstream.database, schema=upstream.schema, identifier=upstream.alias) %}
            {% call statement('register_' ~ upstream.alias) -%}
              CREATE SCHEMA IF NOT EXISTS {{ rel.without_identifier() }};
              CREATE OR REPLACE VIEW {{ rel }} AS
              SELECT * FROM {{ lake_read(upstream.config.location, upstream.config.partition_by) }}
            {%- endcall %}
          {% endif %}
        {% endfor %}
      {% endif %}
    {% endfor %}
    {% if done %}{% do adapter.commit() %}{% endif %}
  {% endif %}
{% endmacro %}
