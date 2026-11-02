#!/usr/bin/env bash
# Live demo source: creates the table public.stores in Postgres, the Debezium connector for it and, through the
# connector, the compacted topic pg_stores. Tableflow is NOT enabled here: that is the live step of the demo.
# Usage: scripts/new-source.sh [up|down]     (CONNECTOR=<name> to use another connector name, see "down")
set -euo pipefail
cd "$(dirname "$0")/.."
source env.sh; source secrets.env
KC="kubectl --context kind-$KIND_CLUSTER"
CONNECTOR=${CONNECTOR:-postgres-stores-cdc-shop}
PSQL="$KC -n data-sources exec -i statefulset/postgres -- psql -U postgres -d shop -v ON_ERROR_STOP=1 -q"
ENABLE="confluent tableflow topic enable pg_stores --cluster $CLUSTER_ID --environment $ENV_ID --storage-type BYOS --provider-integration $PI_ID --bucket-name $BUCKET --table-formats ICEBERG"

case "${1:-up}" in
up)
  $PSQL < k8s/data-sources/stores.sql
  echo "1/3 table public.stores (3 rows) and publication dbz_publication_stores ready"

  # Rendered in memory, secrets never touch disk
  envsubst '$KAFKA_BOOTSTRAP $KAFKA_KEY $KAFKA_SECRET $MYSQL_CDC_PW $PG_CDC_PW' < connectors/postgres-stores-cdc.json | \
    $KC -n connect exec -i deploy/connect -- sh -c 'code=$(curl -sS -o /tmp/resp -w "%{http_code}" -X PUT \
      -H "Content-Type: application/json" --data @- http://localhost:8083/connectors/'"$CONNECTOR"'/config); \
      if [ "$code" -lt 300 ]; then echo "HTTP $code ok"; else echo "HTTP $code"; cat /tmp/resp; echo; exit 1; fi'
  state=""
  for _ in $(seq 1 30); do
    state=$($KC -n connect exec deploy/connect -- curl -s "http://localhost:8083/connectors/$CONNECTOR/status" | jq -r '.tasks[0].state // empty' || true)
    [[ "$state" == RUNNING ]] && break; sleep 2
  done
  echo "2/3 connector $CONNECTOR: ${state:-no task yet (check make connectors-status)}"

  found=""
  for _ in $(seq 1 15); do
    confluent kafka topic describe pg_stores --cluster "$CLUSTER_ID" --environment "$ENV_ID" >/dev/null 2>&1 && { found=1; break; }; sleep 2
  done
  if [[ -n "$found" ]]; then echo "3/3 topic pg_stores created"; else echo "3/3 topic pg_stores not visible yet, check the Console in a few seconds"; fi
  echo
  echo "Enable Tableflow (live step):"
  echo "  $ENABLE"
  ;;
down)
  $KC -n connect exec deploy/connect -- curl -s -X DELETE "http://localhost:8083/connectors/$CONNECTOR" >/dev/null || true
  sleep 5   # the replication slot stays active for a moment after the connector stops
  $PSQL -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name='dbz_slot_stores';" \
        -c "DROP PUBLICATION IF EXISTS dbz_publication_stores;" -c "DROP TABLE IF EXISTS public.stores;"
  echo "Connector, slot, publication and table removed. In Confluent Cloud (not done here):"
  echo "  confluent tableflow topic disable pg_stores --cluster $CLUSTER_ID --environment $ENV_ID"
  echo "  confluent kafka topic delete pg_stores --cluster $CLUSTER_ID --environment $ENV_ID"
  echo "  confluent schema-registry schema delete --subject pg_stores-key --version all --environment $ENV_ID   # then the same with --permanent"
  echo "  confluent schema-registry schema delete --subject pg_stores-value --version all --environment $ENV_ID # then the same with --permanent"
  echo "To run it again, use another connector name (Debezium keeps offsets per name and would skip the snapshot): CONNECTOR=postgres-stores-cdc-shop2 make new-source"
  ;;
*) echo "usage: $0 [up|down]"; exit 1 ;;
esac
