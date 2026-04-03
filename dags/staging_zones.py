"""
# NYC Zones Staging

Reads the raw taxi zone lookup CSV from MinIO, applies cleaning and type casting,
and writes a parquet file to the staging layer.

Runs once, triggered after the raw zones ingestion DAG completes.
"""

from airflow.decorators import dag, task
import logging
from utils.default import get_default_args
from utils.staging.zones import stage_zones

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BUCKET = "data-lake-nyc"


@dag(
    **get_default_args(
        dag_id="zones_staging",
        description="One-time staging pipeline for NYC taxi zone reference data",
        schedule="@once",
        dag_file=__file__,
        doc_md=__doc__,
    )
)
def zones_staging_pipeline():

    @task
    def stage_zone_lookup():
        stage_zones(bucket=BUCKET)

    stage_zone_lookup()


pipeline = zones_staging_pipeline()
