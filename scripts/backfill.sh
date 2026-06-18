#!/usr/bin/env bash
#
# Backfill the full NYC taxi pipeline for a date range.
#
# Wraps the manual multi-step process documented in the README into a single
# command. Each stage waits for the previous one to finish before starting,
# so the ordering constraints (weather staged before taxi, raw registered last)
# are guaranteed without watching the UI.
#
# Usage:
#   ./scripts/backfill.sh <FROM_DATE> <TO_DATE>
#   ./scripts/backfill.sh 2025-04-01 2026-04-01
#
set -euo pipefail

FROM_DATE="${1:?Usage: backfill.sh <FROM_DATE> <TO_DATE> (e.g. 2025-04-01 2026-04-01)}"
TO_DATE="${2:?Usage: backfill.sh <FROM_DATE> <TO_DATE> (e.g. 2025-04-01 2026-04-01)}"

SCHEDULER="nyc_airflow_scheduler"
POLL_INTERVAL=15
# How long to wait for a run to appear after a trigger before assuming the DAG
# already finished. Covers scheduler lag and asset-event propagation (taxi_final).
START_TIMEOUT=120

# Run an Airflow CLI command inside the scheduler container. The env vars mute
# the CLI's startup log noise at the source so the progress output stays readable
# (real errors are still ERROR level and exit codes are preserved).
af() {
    docker exec \
        -e PYTHONWARNINGS=ignore \
        -e AIRFLOW__LOGGING__LOGGING_LEVEL=ERROR \
        "$SCHEDULER" airflow "$@"
}

# Print a timestamped progress line so the current stage is visible in tail -f.
log() {
    echo ">>> [$(date +%H:%M:%S)] $*"
}

# Count the DAG's runs currently in the given state. With -o plain, data rows
# start with the dag_id; anchoring the grep skips the header and "No data found".
count_runs() {
    af dags list-runs "$1" --state "$2" -o plain 2>/dev/null | grep -c "^$1" || true
}

# Block until the given DAG has finished, then abort if it produced a new failed
# run. Two phases: first wait for a run to appear (the scheduler/asset event lags
# the trigger, so a naive "no runs == done" check would pass instantly and skip
# ahead), then wait for it to drain. The failed check compares against a baseline
# taken up front, so pre-existing failures from earlier runs don't trip it.
wait_for_dag() {
    local dag_id="$1"
    local failed_before
    failed_before=$(count_runs "$dag_id" failed)
    echo "Waiting for $dag_id..."

    local waited=0
    while [ "$waited" -lt "$START_TIMEOUT" ]; do
        if [ "$(count_runs "$dag_id" running)" -gt 0 ] || [ "$(count_runs "$dag_id" queued)" -gt 0 ]; then
            break
        fi
        sleep "$POLL_INTERVAL"
        waited=$((waited + POLL_INTERVAL))
    done

    while true; do
        local running queued
        running=$(count_runs "$dag_id" running)
        queued=$(count_runs "$dag_id" queued)
        if [ "$running" -eq 0 ] && [ "$queued" -eq 0 ]; then
            break
        fi
        echo "  $dag_id: $running running, $queued queued..."
        sleep "$POLL_INTERVAL"
    done

    if [ "$(count_runs "$dag_id" failed)" -gt "$failed_before" ]; then
        echo "ERROR: $dag_id produced a failed run. Aborting backfill." >&2
        exit 1
    fi
    echo "  $dag_id finished."
}

log "=== Backfill $FROM_DATE -> $TO_DATE ==="

# 1. Reference data (zones) — staging needs it for zone enrichment.
log "[1/6] Reference data (zones)"
af dags unpause zones_staging
af dags unpause zones_ingestion          # @once: auto-runs when unpaused
wait_for_dag zones_ingestion
wait_for_dag zones_staging               # triggered by the zones asset

# 2. Pause taxi/weather staging so the backfill doesn't fire them per asset event.
log "[2/6] Pausing staging during ingestion"
af dags pause taxi_staging
af dags pause weather_staging

# 3. Backfill ingestion. skip_repair avoids a Spark session per month; the raw
#    catalog is registered once at the end. --max-active-runs 2 throttles the
#    source CDN to avoid HTTP 403 rate limiting.
log "[3/6] Backfilling ingestion"
af dags unpause taxi_ingestion
af dags unpause weather_ingestion
af backfill create --dag-id taxi_ingestion --from-date "$FROM_DATE" --to-date "$TO_DATE" \
    --max-active-runs 2 --dag-run-conf '{"skip_repair": true}'
af backfill create --dag-id weather_ingestion --from-date "$FROM_DATE" --to-date "$TO_DATE" \
    --max-active-runs 2 --dag-run-conf '{"skip_repair": true}'
wait_for_dag taxi_ingestion
wait_for_dag weather_ingestion

# 4. Stage weather FIRST. taxi_final reads staging.weather but only waits on the
#    taxi staging assets, so staging.weather must exist before taxi_staging runs.
log "[4/6] Staging weather"
af dags unpause taxi_final
af dags unpause weather_staging
af dags trigger weather_staging
wait_for_dag weather_staging

# 5. Stage taxi. Auto-discovers all missing partitions in one pass, then
#    taxi_final fires automatically via the staging assets.
log "[5/6] Staging taxi"
af dags unpause taxi_staging
af dags trigger taxi_staging
wait_for_dag taxi_staging
wait_for_dag taxi_final

# 6. Register the raw catalog (skipped during ingestion via skip_repair).
log "[6/6] Registering raw catalog"
af dags unpause hive_setup_raw
af dags trigger hive_setup_raw
wait_for_dag hive_setup_raw

log "=== Backfill complete ==="
