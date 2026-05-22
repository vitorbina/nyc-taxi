import os

PARTITION_COL = "partition_date"

STAGING_DATABASE = "staging"
RAW_DATABASE = "raw"
FINAL_DATABASE = "final"

DEFAULT_BUCKET = os.getenv("MINIO_BUCKET")
