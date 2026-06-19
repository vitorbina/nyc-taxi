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
