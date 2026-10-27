#!/usr/bin/env bash
# Exposes the kind MySQL/Postgres NodePorts (host 13306/15432) to Confluent Cloud via ngrok TCP tunnels,
# then prints the MYSQL_HOST/MYSQL_PORT/PG_HOST/PG_PORT values to put in env.sh.
# Needs: ngrok authtoken configured and ngrok.yml (see ngrok.yml.example). Keep this running during the demo.
set -euo pipefail
cd "$(dirname "$0")/.."

ngrok start --all --config ngrok.yml --log=stdout >/tmp/ngrok.log &
NGROK_PID=$!
trap 'kill $NGROK_PID 2>/dev/null' EXIT

for _ in $(seq 1 30); do
  curl -sf http://127.0.0.1:4040/api/tunnels >/tmp/ngrok-tunnels.json && \
    [[ $(jq '.tunnels|length' /tmp/ngrok-tunnels.json) -ge 2 ]] && break
  sleep 1
done
[[ $(jq '.tunnels|length' /tmp/ngrok-tunnels.json) -ge 2 ]] || { echo "ngrok tunnels did not come up, see /tmp/ngrok.log"; exit 1; }

addr() { jq -r --arg n "$1" '.tunnels[]|select(.name==$n)|.public_url|sub("tcp://";"")' /tmp/ngrok-tunnels.json; }
M=$(addr mysql); P=$(addr postgres)
echo "Update env.sh with:"
echo "export MYSQL_HOST=${M%%:*}"; echo "export MYSQL_PORT=${M##*:}"
echo "export PG_HOST=${P%%:*}";    echo "export PG_PORT=${P##*:}"
echo "Tunnels are up. Ctrl-C to stop."
wait $NGROK_PID
