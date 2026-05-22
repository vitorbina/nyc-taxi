from utils.constants import PARTITION_COL


def raw_key(table: str, partition: str = None, file_name: str = None) -> str:
    parts = ["raw", table]
    if partition:
        parts.append(f"{PARTITION_COL}={partition}")
    if file_name:
        parts.append(file_name)
    return "/".join(parts)


def staging_key(table: str, partition: str = None) -> str:
    parts = ["staging", table]
    if partition:
        parts.append(f"{PARTITION_COL}={partition}")
    return "/".join(parts)


def final_key(table: str) -> str:
    return f"final/{table}"


def s3a(bucket: str, key: str) -> str:
    return f"s3a://{bucket}/{key}"
