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

# Cache chart results. Without it every dashboard load re-scans ~47M trips in
# Trino and each chart takes 30-40s. The final layer only changes when taxi_final
# runs, so a day-long cache is safe; "Force refresh" on a chart bypasses it.
DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "FileSystemCache",
    "CACHE_DIR": "/app/superset_home/cache/data",
    "CACHE_DEFAULT_TIMEOUT": 60 * 60 * 24,
    "CACHE_KEY_PREFIX": "superset_data_",
}

# The zone map draws Stadia Maps "Alidade Smooth" raster tiles (light gray, with
# street and neighborhood names) via Superset 6's `tile://` styles. Stadia serves
# localhost without an API key; a deployment on a real domain needs a free Stadia
# key. (CartoDB now answers keyless requests with an "API KEY REQUIRED" watermark;
# OSM's default tiles have light-blue water that blends with a blue choropleth.)
# Superset's default CSP doesn't list these tile hosts, so the browser would block
# them (blank map): extend img-src/connect-src, keeping the rest of the policy
# (including the script-src nonce) intact.
try:
    from superset.config import TALISMAN_CONFIG

    _TILE_HOSTS = ["https://tiles.stadiamaps.com", "https://tile.openstreetmap.org"]
    _csp = TALISMAN_CONFIG.get("content_security_policy") or {}
    for _directive in ("img-src", "connect-src"):
        _values = _csp.get(_directive)
        if isinstance(_values, list):
            _csp[_directive] = _values + _TILE_HOSTS
    TALISMAN_CONFIG["content_security_policy"] = _csp
except Exception:
    pass

# Light theme only. Superset 6 otherwise follows the OS dark mode, and the
# dashboard's palette and markdown text were validated against a light surface.
# UI theme administration is off so the themes stored in the metadata DB (which
# include a dark one) don't override this config.
THEME_DARK = None
ENABLE_UI_THEME_ADMINISTRATION = False
