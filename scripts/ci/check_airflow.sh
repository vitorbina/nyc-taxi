#!/usr/bin/env bash
# CI check, run inside the Airflow image (see .github/workflows/ci.yml):
#   1. every DAG imports without errors
#   2. the dbt project parses (refs, sources, YAML, Jinja)
# No services needed: Airflow uses a throwaway SQLite database, dbt doesn't connect.
set -euo pipefail

export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:////tmp/airflow.db"
export AIRFLOW__CORE__LOAD_EXAMPLES=false
export PYTHONPATH=/opt/airflow
# dbt's profile reads these; parse doesn't connect, so any value works
export MINIO_ROOT_USER=ci MINIO_ROOT_PASSWORD=ci MINIO_BUCKET=ci MINIO_ENDPOINT=http://minio:9000
# The repo is mounted read-only: dbt writes its logs and artifacts elsewhere
export DBT_LOG_PATH=/tmp/dbt_logs DBT_TARGET_PATH=/tmp/dbt_target

echo "::group::airflow db migrate"
airflow db migrate > /tmp/migrate.log 2>&1 || { cat /tmp/migrate.log; exit 1; }
echo "::endgroup::"

echo "== DAG import check"
python - <<'PY'
from airflow.models.dagbag import DagBag

bag = DagBag("/opt/airflow/dags", include_examples=False)
for path, error in bag.import_errors.items():
    print(f"::error file={path}::{error}")
print(f"{len(bag.dags)} DAGs loaded, {len(bag.import_errors)} import errors")
raise SystemExit(1 if bag.import_errors or not bag.dags else 0)
PY

echo "== dbt parse"
cd /opt/airflow/dbt
/opt/dbt-venv/bin/dbt parse --profiles-dir .
