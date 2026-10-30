#!/usr/bin/env bash
# Creates the federated EXTERNAL catalog (-> Tableflow IRC) and a read-only principal in Polaris.
# Opens its own port-forward to Polaris if localhost:8181 is not already reachable.
# Re-run after any Polaris restart (in-memory persistence).
set -euo pipefail
source env.sh; source secrets.env

P=${POLARIS_URL:-http://localhost:8181}
if [[ "$(curl -s -o /dev/null -w '%{http_code}' "$P/api/catalog/v1/oauth/tokens" || true)" == 000 ]]; then
  kubectl --context "kind-$KIND_CLUSTER" -n polaris port-forward svc/polaris 8181:8181 >/dev/null 2>&1 &
  trap 'kill $! 2>/dev/null' EXIT
  for _ in $(seq 1 20); do sleep 1; [[ "$(curl -s -o /dev/null -w '%{http_code}' "$P/api/catalog/v1/oauth/tokens" || true)" != 000 ]] && break; done
fi
MGMT="$P/api/management/v1"

ROOT_TOKEN=$(curl -sf -X POST "$P/api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials -d client_id=root -d "client_secret=$POLARIS_ROOT_SECRET" \
  -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
H=(-H "Authorization: Bearer $ROOT_TOKEN" -H "Content-Type: application/json")

call() { # method path body ; prints status and body on failure, tolerates 409 (already exists)
  local code
  code=$(curl -s -o /tmp/polaris.out -w '%{http_code}' -X "$1" "$MGMT$2" "${H[@]}" ${3:+-d "$3"})
  if [[ "$code" != 2* && "$code" != 409 ]]; then echo "FAILED $1 $2 -> $code"; cat /tmp/polaris.out; echo; exit 1; fi
  echo "$1 $2 -> $code"
}

LOCATION="s3://${BUCKET}/"

# Federated catalog. remoteCatalogName must stay unset: Tableflow IRC accepts only an empty prefix.
call POST /catalogs "$(cat <<JSON
{
  "catalog": {
    "type": "EXTERNAL",
    "name": "tableflow",
    "properties": {"default-base-location": "${LOCATION}"},
    "storageConfigInfo": {
      "storageType": "S3",
      "region": "${AWS_REGION}",
      "roleArn": "${POLARIS_ROLE_ARN}",
      "allowedLocations": ["${LOCATION}"]
    },
    "connectionConfigInfo": {
      "connectionType": "ICEBERG_REST",
      "uri": "${IRC}",
      "authenticationParameters": {
        "authenticationType": "OAUTH",
        "tokenUri": "${IRC}/v1/oauth/tokens",
        "clientId": "${TF_KEY}",
        "clientSecret": "${TF_SECRET}",
        "scopes": ["catalog"]
      }
    }
  }
}
JSON
)"

# Read-only principal. TABLE_READ_DATA is required for Polaris to vend storage credentials.
code=$(curl -s -o /tmp/principal.json -w '%{http_code}' -X POST "$MGMT/principals" "${H[@]}" -d '{"principal":{"name":"engine_reader"}}')
if [[ "$code" == 2* ]]; then
  jq -r '"export POLARIS_READER_ID=\(.credentials.clientId)\nexport POLARIS_READER_SECRET=\(.credentials.clientSecret)"' /tmp/principal.json > .polaris-reader.env
  echo "principal created, credentials in .polaris-reader.env"
else
  echo "principal call -> $code (already exists? delete it in Polaris or restart Polaris to reset)"; cat /tmp/principal.json; echo
fi

call POST /principal-roles '{"principalRole":{"name":"tableflow_reader"}}'
call PUT  /principals/engine_reader/principal-roles '{"principalRole":{"name":"tableflow_reader"}}'
call POST /catalogs/tableflow/catalog-roles '{"catalogRole":{"name":"reader"}}'
for PRIV in NAMESPACE_LIST NAMESPACE_READ_PROPERTIES TABLE_LIST TABLE_READ_PROPERTIES TABLE_READ_DATA; do
  call PUT /catalogs/tableflow/catalog-roles/reader/grants "{\"grant\":{\"type\":\"catalog\",\"privilege\":\"$PRIV\"}}"
done
call PUT /principal-roles/tableflow_reader/catalog-roles/tableflow '{"catalogRole":{"name":"reader"}}'
echo "Done."
