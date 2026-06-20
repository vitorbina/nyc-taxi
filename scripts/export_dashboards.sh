#!/usr/bin/env bash
set -e

DASHBOARDS_DIR="$(cd "$(dirname "$0")/../dashboards" && pwd)"
EXPORTS_DIR="$DASHBOARDS_DIR/exports"

mkdir -p "$EXPORTS_DIR"

for dashboard in "$DASHBOARDS_DIR"/*/; do
    name="$(basename "$dashboard")"
    [ "$name" = "exports" ] && continue

    zip_path="$EXPORTS_DIR/${name}.zip"
    rm -f "$zip_path"
    (cd "$DASHBOARDS_DIR" && zip -r "$zip_path" "$name/")
    echo "Exported: $zip_path"
done
