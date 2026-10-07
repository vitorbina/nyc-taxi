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

# The zone map's basemap is a Mapbox style: Superset 5 cannot render other tile
# providers. Superset reads MAPBOX_API_KEY from the environment (see .env.example);
# its default CSP already allows api.mapbox.com.
