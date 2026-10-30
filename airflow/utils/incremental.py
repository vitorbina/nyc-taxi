"""
Which partitions (months) a layer needs to rebuild, worked out from the lake itself.

A partition is stale when a target table doesn't have it yet, or when a source wrote
that partition after the target did (a month re-downloaded, or rebuilt upstream).
Comparing the files, rather than passing the month along from the DAG that wrote it,
lets any run catch up on what it missed: a backfill, a failed run, or a month the TLC
republished.

The months found here are handed to dbt (var `partitions`, see dbt/macros/lake.sql),
which rebuilds only those, one at a time.
"""

import logging
from datetime import datetime

from utils.s3 import partition_mtimes

logger = logging.getLogger(__name__)


def stale_partitions(bucket: str, sources: list[str], targets: list[str], full_refresh: bool = False) -> list[str]:
    """Partitions of any source that some target lacks or that changed after it.

    With several targets (tables built in the same run), a partition is stale if it is
    stale for any of them, so a run that failed halfway is redone. full_refresh returns
    every source partition, for when the transformation logic itself changed.
    """
    newest_source: dict[str, datetime] = {}
    for prefix in sources:
        for partition, mtime in partition_mtimes(bucket, prefix).items():
            newest_source[partition] = max(mtime, newest_source.get(partition, mtime))

    if full_refresh:
        stale = sorted(newest_source)
    else:
        built = [partition_mtimes(bucket, prefix) for prefix in targets]
        stale = sorted(
            p for p, mtime in newest_source.items()
            if any(p not in target or target[p] < mtime for target in built)
        )

    logger.info(
        "%d of %d partitions to build%s: %s",
        len(stale), len(newest_source), " (full refresh)" if full_refresh else "", stale,
    )
    return stale
