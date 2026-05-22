from datetime import datetime, timedelta


def get_dag_config(**kwargs) -> dict:
    if "schedule" not in kwargs:
        raise ValueError("A schedule must be provided!")
    if "dag_id" not in kwargs:
        raise ValueError("A dag_id must be provided!")

    default_args = {
        "owner": kwargs.get("owner", "data_engineering"),
        "depends_on_past": kwargs.get("depends_on_past", False),
        "retries": kwargs.get("retries", 2),
        "retry_delay": kwargs.get("retry_delay", timedelta(minutes=5)),
        "start_date": kwargs.get("start_date", datetime(2020, 1, 1)),
    }

    return {
        "dag_id": kwargs["dag_id"],
        "schedule": kwargs["schedule"],
        "default_args": default_args,
        "catchup": kwargs.get("catchup", False),
        "is_paused_upon_creation": kwargs.get("is_paused_upon_creation", True),
        "tags": kwargs.get("tags", ["nyc-taxi"]),
        "description": kwargs.get("description", ""),
        "doc_md": kwargs.get("doc_md", ""),
        "max_active_tasks": kwargs.get("max_active_tasks", 2),
    }
