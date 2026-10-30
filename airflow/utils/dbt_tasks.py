"""
Run dbt models as Airflow tasks with Astronomer Cosmos.

Cosmos reads the dbt project graph and renders one task per selected model, with
that model's tests right after it: a failing test stops everything downstream.
dbt lives in its own virtualenv in the image (/opt/dbt-venv), so Cosmos calls it
as a subprocess.
"""

from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import InvocationMode

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_EXECUTABLE = "/opt/dbt-venv/bin/dbt"

# A DuckDB job can use several GB, so every dbt task takes the single slot of this
# pool (created by airflow-init): they run one at a time across all DAGs.
DUCKDB_POOL = "duckdb"


def dbt_task_group(group_id: str, select: list[str], partitions_from: str | None = None) -> DbtTaskGroup:
    """dbt models as tasks. partitions_from: id of an upstream task that returns the
    partitions to build (utils/incremental.py); without it, models rebuild in full."""
    operator_args = {"pool": DUCKDB_POOL}
    if partitions_from:
        # Rendered at run time, so dbt gets the months that task found stale in this run
        operator_args["vars"] = {
            "partitions": f"{{{{ ti.xcom_pull(task_ids='{partitions_from}') | join(',') }}}}"
        }

    return DbtTaskGroup(
        group_id=group_id,
        project_config=ProjectConfig(DBT_PROJECT_DIR, install_dbt_deps=False),
        profile_config=ProfileConfig(
            profile_name="nyc_taxi",
            target_name="dev",
            profiles_yml_filepath=f"{DBT_PROJECT_DIR}/profiles.yml",
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE,
            invocation_mode=InvocationMode.SUBPROCESS,
        ),
        render_config=RenderConfig(
            select=select,
            # Building the task graph also runs dbt (`dbt ls`), from the same virtualenv
            dbt_executable_path=DBT_EXECUTABLE,
            invocation_mode=InvocationMode.SUBPROCESS,
            # Tests on several models (e.g. the marts reconcile test) get their own
            # task after all of their models, instead of running after the first one
            should_detach_multiple_parents_tests=True,
        ),
        operator_args=operator_args,
    )
