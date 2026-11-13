"""
Superset side of the access personas (run by scripts/create_personas.sh).

- data_engineer: Alpha + sql_lab. Explores any dataset, builds charts, uses SQL Lab.
- analyst: Gamma + nyc_taxi_viewer. Only reads the datasets the NYC Taxi dashboard
  uses, so the dashboard works and nothing else is visible; no SQL Lab.

Idempotent: re-running it recomputes the viewer role from the dashboard (charts and
native filters) and resets both users' roles and passwords to what .env says.
"""

import json
import os

from superset.app import create_app

DASHBOARD = "NYC Taxi"
VIEWER_ROLE = "nyc_taxi_viewer"
USERS = {
    "data_engineer": {"roles": ["Alpha", "sql_lab"], "password_env": "SUPERSET_ENGINEER_PASSWORD"},
    "analyst": {"roles": ["Gamma", VIEWER_ROLE], "password_env": "SUPERSET_ANALYST_PASSWORD"},
}

app = create_app()
with app.app_context():
    from superset import db, security_manager as sm
    from superset.connectors.sqla.models import SqlaTable
    from superset.models.dashboard import Dashboard

    dashboard = db.session.query(Dashboard).filter_by(dashboard_title=DASHBOARD).one_or_none()
    if dashboard is None:
        raise SystemExit(f"Dashboard '{DASHBOARD}' not found: import the dashboards first")

    # Datasets behind the dashboard: its charts plus the ones its native filters read
    dataset_ids = {chart.datasource_id for chart in dashboard.slices}
    for native_filter in json.loads(dashboard.json_metadata or "{}").get("native_filter_configuration", []):
        dataset_ids |= {t["datasetId"] for t in native_filter.get("targets", []) if "datasetId" in t}
    datasets = db.session.query(SqlaTable).filter(SqlaTable.id.in_(dataset_ids)).all()

    role = sm.find_role(VIEWER_ROLE) or sm.add_role(VIEWER_ROLE)
    role.permissions = [
        sm.find_permission_view_menu("datasource_access", d.perm)
        or sm.add_permission_view_menu("datasource_access", d.perm)
        for d in datasets
    ]
    db.session.commit()
    print(f"{VIEWER_ROLE}: {sorted(d.table_name for d in datasets)}")

    for username, spec in USERS.items():
        password = os.environ.get(spec["password_env"])
        if not password:
            raise SystemExit(f"{spec['password_env']} is not set (it comes from .env)")
        roles = [sm.find_role(name) for name in spec["roles"]]
        user = sm.find_user(username=username)
        if user is None:
            sm.add_user(username, username, "persona", f"{username}@nyc-taxi.local", roles, password=password)
        else:
            user.roles = roles
            db.session.commit()
            sm.reset_password(user.id, password)
        print(f"{username}: {spec['roles']}")
