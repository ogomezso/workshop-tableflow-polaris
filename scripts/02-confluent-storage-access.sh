#!/usr/bin/env bash
# Part B4: authorize Confluent's Tableflow Entra app on the ADLS account.
# Prerequisite: create the Azure provider integration in the Confluent Console and export CONFLUENT_APP_ID in env.sh.
set -euo pipefail
source env.sh
: "${CONFLUENT_APP_ID:?set CONFLUENT_APP_ID in env.sh}"

az ad sp create --id "$CONFLUENT_APP_ID" || true      # needs Entra rights; fails harmlessly if it already exists
SP_OID=$(az ad sp show --id "$CONFLUENT_APP_ID" --query id -o tsv)

for ROLE in "Storage Blob Data Contributor" "Reader"; do
  az role assignment create --assignee-object-id "$SP_OID" \
    --assignee-principal-type ServicePrincipal --role "$ROLE" --scope "$SA_ID"
done
echo "Now go back to the Console, enter tenant $TENANT_ID and click Validate."
