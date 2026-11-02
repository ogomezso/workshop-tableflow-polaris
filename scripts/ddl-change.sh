#!/usr/bin/env bash
# Schema change demo on Postgres: adds a nullable column and updates two rows so Debezium emits the new schema.
# Usage: scripts/ddl-change.sh [column]   (default: category; use a new name for every repeat, Tableflow cannot drop columns)
set -euo pipefail
cd "$(dirname "$0")/.."
source env.sh
COLUMN=${1:-category}
[[ "$COLUMN" =~ ^[a-z_][a-z0-9_]*$ ]] || { echo "invalid column name: $COLUMN"; exit 1; }
KC="kubectl --context kind-$KIND_CLUSTER"

$KC -n data-sources exec statefulset/postgres -- psql -U postgres -d shop -v ON_ERROR_STOP=1 \
  -c "ALTER TABLE public.products ADD COLUMN IF NOT EXISTS $COLUMN TEXT;" \
  -c "UPDATE public.products SET $COLUMN='tools' WHERE sku IN ('SKU-A','SKU-B');"

echo "Column $COLUMN added (nullable). Postgres announces it with the first row change, which we just made."
echo "Debezium registers the new schema version; after the next Tableflow commit the Iceberg table has the column."
