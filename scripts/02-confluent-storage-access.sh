#!/usr/bin/env bash
# Part B4: create the Confluent provider integration (AWS) and let Confluent assume the Tableflow role.
# Prerequisites: `confluent login`, ENV_ID in env.sh, and `make aws` already done. Safe to re-run.
set -euo pipefail
cd "$(dirname "$0")/.."
source env.sh
source scripts/lib.sh
: "${ENV_ID:?set ENV_ID in env.sh (BUILD_GUIDE.md Part B1)}"
: "${AWS_ACCOUNT_ID:?run make aws first}"

# The CLI JSON field names are not documented, so match keys ignoring case, '_', '-' and spaces
pick() { # json normalized-key
  jq -r --arg k "$2" '[.. | objects | to_entries[] | select((.key | ascii_downcase | gsub("[ _-]"; "")) == $k) | .value | strings][0] // empty' <<<"$1"
}

if [[ -z "${PI_ID:-}" ]]; then
  OUT=$(confluent provider-integration create "tableflow-aws-$SUFFIX" --cloud aws \
        --customer-role-arn "$TABLEFLOW_ROLE_ARN" --environment "$ENV_ID" -o json)
  PI_ID=$(pick "$OUT" id)
  [[ -n "$PI_ID" ]] || { echo "could not read the integration id from:"; echo "$OUT"; exit 1; }
  setvar env.sh PI_ID "$PI_ID"
  echo "created provider integration $PI_ID"
else
  echo "provider integration $PI_ID already set in env.sh"
fi

DESC=$(confluent provider-integration describe "$PI_ID" --environment "$ENV_ID" -o json)
ROLE=${CONFLUENT_IAM_ROLE_ARN:-$(pick "$DESC" iamrolearn)}
EXT=${CONFLUENT_EXTERNAL_ID:-$(pick "$DESC" externalid)}
if [[ -z "$ROLE" || -z "$EXT" ]]; then
  echo "could not read Confluent's IAM role ARN / external ID from:"; echo "$DESC"
  echo "Copy them (also shown in Console > Provider integrations) into CONFLUENT_IAM_ROLE_ARN and CONFLUENT_EXTERNAL_ID in env.sh and run this again."
  exit 1
fi

# Confluent's role can take a minute to become a valid principal in your account: retry for up to ~3 minutes
retry 18 10 aws iam update-assume-role-policy --role-name "$TABLEFLOW_ROLE" --policy-document "$(jq -nc --arg a "$ROLE" --arg e "$EXT" \
  '{Version:"2012-10-17",Statement:[
     {Effect:"Allow",Principal:{AWS:$a},Action:"sts:AssumeRole",Condition:{StringEquals:{"sts:ExternalId":$e}}},
     {Effect:"Allow",Principal:{AWS:$a},Action:"sts:TagSession"}]}')"
setvar env.sh CONFLUENT_IAM_ROLE_ARN "$ROLE"
setvar env.sh CONFLUENT_EXTERNAL_ID "$EXT"
echo "trust policy of $TABLEFLOW_ROLE now allows $ROLE (AssumeRole with external id + TagSession); waiting 15s for IAM propagation"
sleep 15
echo "Done. If Tableflow was already FAILED with an AssumeRole error: confluent tableflow topic disable <topic> and make tableflow-orders again."
