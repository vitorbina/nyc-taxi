from datetime import datetime, timedelta


def get_default_args(**kwargs):

    assert "schedule" in kwargs, "A schedule must be provided!"
    assert "dag_id" in kwargs, "A dag_id must be provided!"

    base_default_args = {
        "owner": kwargs.get("owner", "data_engineering"),
        "depends_on_past": kwargs.get("depends_on_past", False),
        "retries": kwargs.get("retries", 2),
        "retry_delay": kwargs.get("retry_delay", timedelta(minutes=5)),
        "start_date": kwargs.get("start_date", datetime(2026, 4, 1)),
        "dag_file": kwargs.get("dag_file", ""),
    }

    dag_config = {
        "dag_id": kwargs["dag_id"],
        "schedule": kwargs["schedule"],
        "default_args": base_default_args,
        "catchup": kwargs.get("catchup", False),
        "is_paused_upon_creation": kwargs.get("is_paused_upon_creation", True),
        "tags": kwargs.get("tags", ["nyc_taxi"]),
        "description": kwargs.get("description", ""),
        "doc_md": kwargs.get("doc_md", ""),
    }

    return dag_config
