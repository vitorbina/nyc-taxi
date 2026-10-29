"""
Register lake tables in the Hive Metastore, through Trino.

The schema of a Parquet table is read from the files themselves with DuckDB (only
the footers are fetched), then Trino creates the external table and syncs its
partitions with the folders found in MinIO. Dropping an external table only
removes the catalog entry, never the files.
"""

import logging
import os

import duckdb
import trino

from utils.s3 import folder_exists, list_partitions
from utils.constants import PARTITION_COL, STAGING_DATABASE, DEFAULT_BUCKET

logger = logging.getLogger(__name__)

CATALOG = "hive"

# DuckDB type (as read from Parquet) -> Hive type in Trino
_TRINO_TYPES = {
    "VARCHAR": "varchar",
    "BOOLEAN": "boolean",
    "TINYINT": "tinyint",
    "SMALLINT": "smallint",
    "INTEGER": "integer",
    "BIGINT": "bigint",
    "FLOAT": "real",
    "DOUBLE": "double",
    "DATE": "date",
    "TIMESTAMP": "timestamp(3)",
    "TIMESTAMP_NS": "timestamp(3)",
    "BLOB": "varbinary",
}


def _trino_type(duckdb_type: str) -> str:
    if duckdb_type.startswith("DECIMAL"):
        return duckdb_type.lower()
    if duckdb_type not in _TRINO_TYPES:
        raise ValueError(f"No Hive type mapped for DuckDB type {duckdb_type}")
    return _TRINO_TYPES[duckdb_type]


def _parquet_schema_ddl(bucket: str, prefix: str, is_partitioned: bool) -> str:
    con = duckdb.connect()
    try:
        con.sql("LOAD httpfs")
        con.execute(
            "CREATE SECRET (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, URL_STYLE 'path', USE_SSL false)",
            [
                os.getenv("MINIO_ROOT_USER"),
                os.getenv("MINIO_ROOT_PASSWORD"),
                os.getenv("MINIO_ENDPOINT").replace("http://", ""),
            ],
        )
        files = f"s3://{bucket}/{prefix}" + ("*/*.parquet" if is_partitioned else "*.parquet")
        # union_by_name: raw TLC files gained columns over the years
        columns = con.sql(
            f"DESCRIBE SELECT * FROM read_parquet('{files}', hive_partitioning=false, union_by_name=true)"
        ).fetchall()
    finally:
        con.close()

    # Hive column names are case-insensitive; raw files have both Airport_fee and airport_fee
    seen, fields = set(), []
    for name, duckdb_type, *_ in columns:
        if name.lower() not in seen:
            seen.add(name.lower())
            fields.append(f'"{name.lower()}" {_trino_type(duckdb_type)}')
    return ", ".join(fields)


def _connect():
    return trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user="airflow",
        catalog=CATALOG,
    )


def _run(cur, sql: str) -> list:
    cur.execute(sql)
    return cur.fetchall()


def _create_schema(cur, database: str, bucket: str) -> None:
    # Without a location the metastore would default to a local (file:) path
    _run(cur, f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{database} WITH (location = 's3a://{bucket}/{database}/')")


def _table_exists(cur, database: str, table: str) -> bool:
    return bool(_run(cur, f"SHOW TABLES FROM {CATALOG}.{database} LIKE '{table}'"))


def _is_partitioned(cur, database: str, table: str) -> bool:
    return bool(_run(cur, f"""
        SELECT 1 FROM {CATALOG}.information_schema.columns
        WHERE table_schema = '{database}' AND table_name = '{table}' AND column_name = '{PARTITION_COL}'
    """))


def _create_external_table(
    cur,
    table: str,
    database: str,
    location_prefix: str,
    bucket: str,
    file_format: str = "parquet",
    schema_ddl: str = None,
) -> None:
    prefix = f"{location_prefix}/{table}/"
    # The metastore validates locations with Hadoop's S3A client, hence s3a://
    location = f"s3a://{bucket}/{prefix}"

    if not folder_exists(bucket, prefix):
        raise FileNotFoundError(
            f"No data found at {location}. "
            f"Run the DAG that produces '{table}' before this task."
        )

    is_partitioned = bool(list_partitions(bucket, prefix))
    if file_format == "json":
        if schema_ddl is None:
            raise ValueError(f"schema_ddl is required when file_format='json' (table: {table})")
    else:
        logger.info("Reading schema from %s...", location)
        schema_ddl = _parquet_schema_ddl(bucket, prefix, is_partitioned)

    properties = [f"external_location = '{location}'", f"format = '{file_format.upper()}'"]
    if is_partitioned:
        # Hive partition columns go last in the column list
        schema_ddl += f', "{PARTITION_COL}" varchar'
        properties.append(f"partitioned_by = ARRAY['{PARTITION_COL}']")

    logger.info("Creating table %s.%s at %s", database, table, location)
    _run(cur, f"""
        CREATE TABLE {CATALOG}.{database}.{table} ({schema_ddl})
        WITH ({", ".join(properties)})
    """)


def _sync_partitions(cur, database: str, table: str) -> None:
    if _is_partitioned(cur, database, table):
        logger.info("Syncing partitions for %s.%s...", database, table)
        # FULL adds new partition folders and drops the ones that no longer exist
        _run(cur, f"CALL {CATALOG}.system.sync_partition_metadata('{database}', '{table}', 'FULL')")
        logger.info("Partitions updated for %s.%s.", database, table)


def repair_table(
    table: str,
    database: str = STAGING_DATABASE,
    location_prefix: str = None,
    bucket: str = DEFAULT_BUCKET,
    file_format: str = "parquet",
    schema_ddl: str = None,
) -> None:
    if location_prefix is None:
        location_prefix = database

    with _connect() as conn:
        cur = conn.cursor()
        _create_schema(cur, database, bucket)

        if not _table_exists(cur, database, table):
            logger.info("Table %s.%s does not exist, creating...", database, table)
            _create_external_table(cur, table, database, location_prefix, bucket, file_format, schema_ddl)

        _sync_partitions(cur, database, table)


def setup_hive(
    tables: list,
    database: str,
    location_prefix: str,
    bucket: str = DEFAULT_BUCKET,
    file_format: str = "parquet",
    schema_ddl: str = None,
) -> None:
    with _connect() as conn:
        cur = conn.cursor()
        logger.info("Creating database %s if not exists...", database)
        _create_schema(cur, database, bucket)

        for table in tables:
            _run(cur, f"DROP TABLE IF EXISTS {CATALOG}.{database}.{table}")
            _create_external_table(cur, table, database, location_prefix, bucket, file_format, schema_ddl)
            _sync_partitions(cur, database, table)
            logger.info("Table %s.%s registered successfully.", database, table)
