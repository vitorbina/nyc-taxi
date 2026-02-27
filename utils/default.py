from datetime import datetime, timedelta

# # This function generates default arguments for Airflow DAGs, allowing for configuration across multiple DAGs
def get_default_args(**kwargs):

    assert "schedule" in kwargs, "A schedule must be provided!"
    assert "dag_id" in kwargs, "A dag_id must be provided!"

    base_default_args = {
        "owner": kwargs.get("owner", "data_engineering"),
        "depends_on_past": kwargs.get("depends_on_past", False),
        "retries": kwargs.get("retries", 2),
        "retry_delay": kwargs.get("retry_delay", timedelta(minutes=5)),
        "start_date": kwargs.get("start_date", datetime(2024, 1, 1)),
    }

    dag_config = {
        "dag_id": kwargs["dag_id"],
        "schedule": kwargs["schedule"],
        "default_args": base_default_args,
        "catchup": kwargs.get("catchup", False),
        "tags": kwargs.get("tags", ["nyc_taxi"]),
        "description": kwargs.get("description", ""),
    }

    return dag_config