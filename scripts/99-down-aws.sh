#!/usr/bin/env bash
# Part H: delete the S3 bucket (with its data) and the IAM user/roles created by 01-aws.sh.
# Disable Tableflow on the topics (or delete the Confluent cluster) FIRST, otherwise Tableflow may write to the bucket again.
set -euo pipefail
cd "$(dirname "$0")/.."
source env.sh
[[ "${1:-}" == --yes ]] || { read -r -p "Delete bucket $BUCKET and IAM $TABLEFLOW_ROLE, $POLARIS_ROLE, $POLARIS_USER? [y/N] " a; [[ "$a" == y ]] || exit 1; }

aws s3 rm "s3://$BUCKET" --recursive || true
aws s3api delete-bucket --bucket "$BUCKET" || true
for r in "$TABLEFLOW_ROLE:tableflow-s3" "$POLARIS_ROLE:read-bucket"; do
  aws iam delete-role-policy --role-name "${r%%:*}" --policy-name "${r##*:}" || true
  aws iam delete-role --role-name "${r%%:*}" || true
done
for k in $(aws iam list-access-keys --user-name "$POLARIS_USER" --query 'AccessKeyMetadata[].AccessKeyId' --output text 2>/dev/null); do
  aws iam delete-access-key --user-name "$POLARIS_USER" --access-key-id "$k"
done
aws iam delete-user-policy --user-name "$POLARIS_USER" --policy-name assume-polaris-role || true
aws iam delete-user --user-name "$POLARIS_USER" || true
echo "AWS resources deleted. Also delete the provider integration: confluent provider-integration delete ${PI_ID:-<cspi-id>} --environment ${ENV_ID:-<env-id>}"
