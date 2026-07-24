#!/usr/bin/env bash
# Idempotent dashboard deploy: rebuild Superset state from the YAML source of truth.
#
# Why delete + reimport: Superset's import (CLI and API, even with overwrite=true)
# does NOT overwrite charts that already exist by UUID -- it only creates missing
# ones and updates the dashboard. So chart-level changes never take effect on a
# plain reimport. Deleting the charts first forces the import to recreate them
# fresh from YAML. UUIDs are preserved in the YAML, so dashboard links restore.
#
# WARNING: any change made only in the Superset UI (and not exported back to YAML)
# is lost. Treat git as the single source of truth; export before you deploy.
set -e

SUPERSET_URL="${SUPERSET_URL:-http://localhost:3000}"
ADMIN_USER="${SUPERSET_ADMIN_USER:-admin}"
ADMIN_PASS="${SUPERSET_ADMIN_PASSWORD:-admin}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DASHBOARDS_DIR="$(cd "$SCRIPT_DIR/../dashboards" && pwd)"
CHARTS_DIR="$DASHBOARDS_DIR/nyc_taxi_overview/charts"
ZIP_PATH="$DASHBOARDS_DIR/exports/nyc_taxi_overview.zip"
COOKIE_JAR="$(mktemp)"
trap 'rm -f "$COOKIE_JAR"' EXIT

echo "==> Exporting YAML to zip..."
"$SCRIPT_DIR/export_dashboards.sh" >/dev/null

echo "==> Logging in as $ADMIN_USER..."
ACCESS_TOKEN=$(curl -s -c "$COOKIE_JAR" -X POST "$SUPERSET_URL/api/v1/security/login" \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"$ADMIN_USER\",\"password\":\"$ADMIN_PASS\",\"provider\":\"db\",\"refresh\":true}" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

CSRF_TOKEN=$(curl -s -b "$COOKIE_JAR" -c "$COOKIE_JAR" "$SUPERSET_URL/api/v1/security/csrf_token/" \
    -H "Authorization: Bearer $ACCESS_TOKEN" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['result'])")

# UUIDs owned by the YAML source of truth.
LOCAL_UUIDS=$(grep -h '^uuid:' "$CHARTS_DIR"/*.yaml | awk '{print $2}')

echo "==> Resolving chart ids to delete..."
ALL_CHARTS=$(curl -s -b "$COOKIE_JAR" "$SUPERSET_URL/api/v1/chart/?q=(page_size:200)" \
    -H "Authorization: Bearer $ACCESS_TOKEN")

IDS_TO_DELETE=$(python3 -c "
import sys, json
local = set('''$LOCAL_UUIDS'''.split())
charts = json.load(sys.stdin)['result']
print(' '.join(str(c['id']) for c in charts if c['uuid'] in local))
" <<< "$ALL_CHARTS")

for id in $IDS_TO_DELETE; do
    echo "    deleting chart $id"
    curl -s -b "$COOKIE_JAR" -X DELETE "$SUPERSET_URL/api/v1/chart/$id" \
        -H "Authorization: Bearer $ACCESS_TOKEN" \
        -H "X-CSRFToken: $CSRF_TOKEN" \
        -H "Referer: $SUPERSET_URL" >/dev/null
done

echo "==> Importing $ZIP_PATH (overwrite=true)..."
curl -s -b "$COOKIE_JAR" -X POST "$SUPERSET_URL/api/v1/dashboard/import/" \
    -H "Authorization: Bearer $ACCESS_TOKEN" \
    -H "X-CSRFToken: $CSRF_TOKEN" \
    -H "Referer: $SUPERSET_URL" \
    -F "formData=@$ZIP_PATH;type=application/zip" \
    -F "overwrite=true"
echo
echo "==> Done."
