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

  A partitioned model is written in one of two ways:
    - var `partitions` set (e.g. --vars '{partitions: "2025-11-01,2025-12-01"}'): only
      those partitions, one COPY each. The SELECT is filtered to one partition, which
      DuckDB pushes down to the folders it reads, so a run touches one month of input at
      a time and memory doesn't grow with the history. Each COPY replaces that
      partition's file (OVERWRITE_OR_IGNORE) and leaves the other partitions alone.
      This is how Airflow runs it, with the months it found stale.
    - no `partitions`: the whole table in one COPY. OVERWRITE clears the directory first,
      so a partition that disappeared from the source doesn't linger.
  Unpartitioned models always rebuild the whole file.
#}

{% macro lake_partitions() -%}
  {%- set requested = var('partitions', none) -%}
  {%- if requested is none or requested == '' -%}{{ return([]) }}{%- endif -%}
  {%- if requested is string -%}
    {%- set requested = requested.split(',') -%}
  {%- elif requested is not iterable -%}
    {#- YAML turns a single bare 2025-12-01 into a date -#}
    {%- set requested = [requested] -%}
  {%- endif -%}
  {%- set partitions = [] -%}
  {%- for p in requested if p | string | trim -%}
    {%- set p = p | string | trim -%}
    {%- if not modules.re.fullmatch('\d{4}-\d{2}-\d{2}', p) -%}
      {{ exceptions.raise_compiler_error("partitions must be YYYY-MM-DD dates, got: " ~ p) }}
    {%- endif -%}
    {%- do partitions.append(p) -%}
  {%- endfor -%}
  {{ return(partitions) }}
{%- endmacro %}

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
  {%- set partitions = lake_partitions() if partition_by else [] -%}
  {%- set target_relation = this.incorporate(type='view') -%}

  {{ run_hooks(pre_hooks) }}

  {% if partitions %}
    {% for p in partitions %}
      {# dbt expects the last statement of a materialization to be called 'main' #}
      {% call statement('main' if loop.last else 'partition_' ~ loop.index) -%}
        COPY (
          SELECT * FROM (
            {{ sql }}
          ) WHERE {{ partition_by }} = '{{ p }}'
        ) TO '{{ location }}' (FORMAT parquet, PARTITION_BY ({{ partition_by }}), OVERWRITE_OR_IGNORE)
      {%- endcall %}
    {% endfor %}
  {% else %}
    {% call statement('main') -%}
      COPY ({{ sql }}) TO '{{ location }}' (
        FORMAT parquet
        {%- if partition_by %}, PARTITION_BY ({{ partition_by }}), OVERWRITE{% endif %}
      )
    {%- endcall %}
  {% endif %}

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
  project runs (`dbt run -s trips`, or one model per Airflow task), the upstream
  `lake` models it refs have no view yet. This on-run-start hook creates those
  views, reading the upstream files back from MinIO. Ephemeral models are inlined
  as CTEs, so the walk goes through them to the lake models they read.
#}
{% macro register_upstream_lake_models() %}
  {% if execute %}
    {% set to_visit = [] %}
    {% for node_id in selected_resources %}
      {% set node = graph.nodes.get(node_id) %}
      {% if node %}{% do to_visit.extend(node.depends_on.nodes) %}{% endif %}
    {% endfor %}

    {% set registered = [] %}
    {# Jinja has no while loop; the project is shallower than 10 ephemeral levels #}
    {% for _ in range(10) %}
      {% set next_level = [] %}
      {% for node_id in to_visit if node_id not in selected_resources and node_id not in registered %}
        {% set node = graph.nodes.get(node_id) %}
        {% if node and node.config.materialized == 'ephemeral' %}
          {% do next_level.extend(node.depends_on.nodes) %}
        {% elif node and node.config.materialized == 'lake' %}
          {% do registered.append(node_id) %}
          {% set rel = api.Relation.create(database=node.database, schema=node.schema, identifier=node.alias) %}
          {% call statement('register_' ~ node.alias) -%}
            CREATE SCHEMA IF NOT EXISTS {{ rel.without_identifier() }};
            CREATE OR REPLACE VIEW {{ rel }} AS
            SELECT * FROM {{ lake_read(node.config.location, node.config.partition_by) }}
          {%- endcall %}
        {% endif %}
      {% endfor %}
      {# `set` inside a loop doesn't outlive the iteration in Jinja; mutate instead #}
      {% do to_visit.clear() %}{% do to_visit.extend(next_level) %}
    {% endfor %}
    {% if registered %}{% do adapter.commit() %}{% endif %}
  {% endif %}
{% endmacro %}
