#!/usr/bin/env bash
# Part A of BUILD_GUIDE.md: resource group, ADLS Gen2 and a service principal for Polaris.
# The only Azure resources besides Confluent Cloud's. Kubernetes runs locally in kind.
set -euo pipefail
source env.sh

az group create -n "$RG" -l "$LOC" --tags purpose=demo owner="${USER:-demo}" >/dev/null

az storage account create -n "$SA" -g "$RG" -l "$LOC" \
  --sku Standard_LRS --kind StorageV2 --hns true --allow-blob-public-access false >/dev/null
az storage container create --account-name "$SA" -n "$CONTAINER" --auth-mode key >/dev/null
SA_ID=$(az storage account show -n "$SA" -g "$RG" --query id -o tsv)

# Polaris runs outside Azure, so it authenticates with a service principal secret (no Workload Identity).
SP_JSON=$(az ad sp create-for-rbac -n "$SP_NAME" --query '{appId:appId,password:password,tenant:tenant}' -o json)
POLARIS_CLIENT_ID=$(jq -r .appId <<<"$SP_JSON")
POLARIS_CLIENT_SECRET=$(jq -r .password <<<"$SP_JSON")
TENANT_ID=$(jq -r .tenant <<<"$SP_JSON")

# Reader + Delegator (user-delegation key for SAS vending): verify against your Polaris version
for ROLE in "Storage Blob Data Reader" "Storage Blob Delegator"; do
  az role assignment create --assignee "$POLARIS_CLIENT_ID" --role "$ROLE" --scope "$SA_ID" >/dev/null
done

echo
echo "Add these to env.sh:"
echo "export TENANT_ID=$TENANT_ID"
echo "export SUB_ID=$(az account show --query id -o tsv)"
echo "export SA_ID=$SA_ID"
echo "export POLARIS_CLIENT_ID=$POLARIS_CLIENT_ID"
echo
echo "Add this to secrets.env:"
echo "export POLARIS_CLIENT_SECRET=$POLARIS_CLIENT_SECRET"
