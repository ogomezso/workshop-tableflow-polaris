#!/usr/bin/env bash
# Creates env.sh and secrets.env from the templates with fixed random values. Never overwrites an existing file.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/lib.sh

if [[ -e env.sh ]]; then
  echo "env.sh already exists, left unchanged"
else
  cp env.sh.example env.sh
  setvar env.sh SUFFIX "$(openssl rand -hex 3)"
  ACCOUNT=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)
  if [[ -n "$ACCOUNT" ]]; then setvar env.sh AWS_ACCOUNT_ID "$ACCOUNT"; fi
  echo "created env.sh (AWS_ACCOUNT_ID: ${ACCOUNT:-not set, log in to AWS and run 'make aws' later})"
fi

if [[ -e secrets.env ]]; then
  echo "secrets.env already exists, left unchanged"
else
  cp secrets.env.example secrets.env
  for v in MYSQL_ROOT_PW MYSQL_CDC_PW PG_PW PG_CDC_PW; do setvar secrets.env "$v" "$(openssl rand -hex 12)"; done
  setvar secrets.env POLARIS_ROOT_SECRET "$(openssl rand -hex 16)"
  chmod 600 secrets.env
  echo "created secrets.env (database passwords and Polaris root secret generated)"
fi

echo
echo "Next: edit AWS_REGION (and OWNER_EMAIL if your org requires an owner tag) in env.sh, then: source env.sh && make aws"
