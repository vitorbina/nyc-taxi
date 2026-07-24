import os

SQLALCHEMY_DATABASE_URI = os.environ["SUPERSET_DB_URI"]
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

SQLLAB_TIMEOUT = 300
SUPERSET_WEBSERVER_TIMEOUT = 300

# Connection pool for Superset's own metadata DB. The defaults (pool_size 5,
# max_overflow 10) get exhausted when a dashboard loads many charts at once,
# raising "QueuePool limit of size 5 overflow 10 reached". Trino uses NullPool,
# so this tuning is about the metadata DB only.
SQLALCHEMY_ENGINE_OPTIONS = {
    "pool_size": 20,
    "max_overflow": 40,
    "pool_timeout": 60,
    "pool_pre_ping": True,
}

# The Revenue by Zone map uses the CartoDB Positron (grayscale) basemap so it
# stays monochrome and readable. Superset's default CSP only whitelists the OSM
# tile hosts, so the browser blocks any other tile provider (blank/black map).
# We extend img-src and connect-src with the CartoDB hosts, preserving the rest
# of the policy (including the script-src nonce) instead of replacing it.
try:
    from superset.config import TALISMAN_CONFIG

    _CARTO_HOSTS = ["https://basemaps.cartocdn.com", "https://*.basemaps.cartocdn.com"]
    _csp = TALISMAN_CONFIG.get("content_security_policy") or {}
    for _directive in ("img-src", "connect-src"):
        _values = _csp.get(_directive)
        if isinstance(_values, list):
            _csp[_directive] = _values + _CARTO_HOSTS
    TALISMAN_CONFIG["content_security_policy"] = _csp
except Exception:
    pass
