#!/usr/bin/env bash
# Part H: delete the S3 bucket (with its data) and the IAM user/roles created by 01-aws.sh.
# Disable Tableflow on the topics (or delete the Confluent cluster) FIRST, otherwise Tableflow may write to the bucket again.
# Only "already gone" errors are ignored; anything else (expired credentials, access denied) stops the script.
set -euo pipefail
cd "$(dirname "$0")/.."
source env.sh
aws sts get-caller-identity >/dev/null || { echo "AWS credentials missing or expired: log in again and rerun."; exit 1; }
[[ "${1:-}" == --yes ]] || { read -r -p "Delete bucket $BUCKET and IAM $TABLEFLOW_ROLE, $POLARIS_ROLE, $POLARIS_USER? [y/N] " a; [[ "$a" == y ]] || exit 1; }

gone() { # run an aws command; tolerate only "does not exist" answers
  local err
  err=$("$@" 2>&1 >/dev/null) && return 0
  if grep -qE 'NoSuchEntity|NoSuchBucket|NotFound|does not exist' <<<"$err"; then return 0; fi
  echo "FAILED: $*"; echo "$err"; exit 1
}

if aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  aws s3 rm "s3://$BUCKET" --recursive >/dev/null
  gone aws s3api delete-bucket --bucket "$BUCKET"
fi
for r in "$TABLEFLOW_ROLE:tableflow-s3" "$POLARIS_ROLE:read-bucket"; do
  gone aws iam delete-role-policy --role-name "${r%%:*}" --policy-name "${r##*:}"
  gone aws iam delete-role --role-name "${r%%:*}"
done
for k in $(aws iam list-access-keys --user-name "$POLARIS_USER" --query 'AccessKeyMetadata[].AccessKeyId' --output text 2>/dev/null || true); do
  aws iam delete-access-key --user-name "$POLARIS_USER" --access-key-id "$k"
done
gone aws iam delete-user-policy --user-name "$POLARIS_USER" --policy-name assume-polaris-role
gone aws iam delete-user --user-name "$POLARIS_USER"
echo "AWS resources deleted (bucket, roles, user). The Confluent provider integration disappears with its environment; if you kept the environment: confluent provider-integration delete ${PI_ID:-<cspi-id>} --environment ${ENV_ID:-<env-id>}"
