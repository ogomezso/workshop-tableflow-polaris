#!/usr/bin/env bash
# Part A of BUILD_GUIDE.md: S3 bucket plus the IAM roles for Tableflow (write) and Polaris (read-only, vended credentials).
# Idempotent: safe to re-run after a failure. Needs an AWS login that can create buckets, IAM roles and IAM users.
set -euo pipefail
cd "$(dirname "$0")/.."
source env.sh
source scripts/lib.sh

[[ "$SUFFIX" != changeme ]] || { echo "SUFFIX is still 'changeme': run 'make init' or set a fixed value in env.sh"; exit 1; }

ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
if [[ -n "${AWS_ACCOUNT_ID:-}" && "$AWS_ACCOUNT_ID" != "$ACCOUNT" ]]; then
  echo "env.sh has AWS_ACCOUNT_ID=$AWS_ACCOUNT_ID but you are logged in to account $ACCOUNT"; exit 1
fi
[[ -n "${AWS_ACCOUNT_ID:-}" ]] || setvar env.sh AWS_ACCOUNT_ID "$ACCOUNT"
USER_ARN="arn:aws:iam::$ACCOUNT:user/$POLARIS_USER"
echo "account $ACCOUNT, region $AWS_REGION, bucket $BUCKET"

TAGS=(Key=purpose,Value=demo)
S3_TAGS="{Key=purpose,Value=demo}"
if [[ -n "${OWNER_EMAIL:-}" ]]; then
  TAGS+=("Key=owner_email,Value=$OWNER_EMAIL")
  S3_TAGS="$S3_TAGS,{Key=owner_email,Value=$OWNER_EMAIL}"
fi


# --- S3 bucket (same region as the Confluent cluster) ---
if aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  echo "bucket $BUCKET exists"
else
  ARGS=(--bucket "$BUCKET" --region "$AWS_REGION")
  [[ "$AWS_REGION" == us-east-1 ]] || ARGS+=(--create-bucket-configuration "LocationConstraint=$AWS_REGION")
  aws s3api create-bucket "${ARGS[@]}" >/dev/null
  echo "created bucket $BUCKET"
fi
aws s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-tagging --bucket "$BUCKET" --tagging "TagSet=[$S3_TAGS]"

# --- IAM user whose access key lets Polaris (running in kind, outside AWS) assume the read-only role ---
aws iam get-user --user-name "$POLARIS_USER" >/dev/null 2>&1 || \
  aws iam create-user --user-name "$POLARIS_USER" --tags "${TAGS[@]}" >/dev/null
aws iam put-user-policy --user-name "$POLARIS_USER" --policy-name assume-polaris-role --policy-document "$(cat <<JSON
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sts:AssumeRole","Resource":"arn:aws:iam::$ACCOUNT:role/$POLARIS_ROLE"}]}
JSON
)"

# --- Role Confluent assumes to write Tableflow data. Trust is a placeholder (this account) until `make confluent-storage`
#     replaces it with Confluent's role + external ID. Never reset it on a re-run. ---
if aws iam get-role --role-name "$TABLEFLOW_ROLE" >/dev/null 2>&1; then
  echo "role $TABLEFLOW_ROLE exists (trust policy left unchanged)"
else
  aws iam create-role --role-name "$TABLEFLOW_ROLE" --tags "${TAGS[@]}" --assume-role-policy-document "$(cat <<JSON
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::$ACCOUNT:root"},"Action":"sts:AssumeRole"}]}
JSON
)" >/dev/null
fi
# Permission policy from the Confluent docs (Tableflow storage, S3 + KMS in the same account)
aws iam put-role-policy --role-name "$TABLEFLOW_ROLE" --policy-name tableflow-s3 --policy-document "$(cat <<JSON
{"Version":"2012-10-17","Statement":[
 {"Effect":"Allow","Action":["s3:GetBucketLocation","s3:ListBucketMultipartUploads","s3:ListBucket"],"Resource":["arn:aws:s3:::$BUCKET"]},
 {"Effect":"Allow","Action":["s3:PutObject","s3:PutObjectTagging","s3:GetObject","s3:DeleteObject","s3:AbortMultipartUpload","s3:ListMultipartUploadParts"],"Resource":["arn:aws:s3:::$BUCKET/*"]}]}
JSON
)"

# --- Read-only role Polaris assumes to vend table-scoped credentials (policy from the internal Polaris federation how-to) ---
POLARIS_TRUST="$(cat <<JSON
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"$USER_ARN"},"Action":"sts:AssumeRole"}]}
JSON
)"
if aws iam get-role --role-name "$POLARIS_ROLE" >/dev/null 2>&1; then
  aws iam update-assume-role-policy --role-name "$POLARIS_ROLE" --policy-document "$POLARIS_TRUST"
else
  retry 8 5 aws iam create-role --role-name "$POLARIS_ROLE" --tags "${TAGS[@]}" --assume-role-policy-document "$POLARIS_TRUST" >/dev/null
  aws iam get-role --role-name "$POLARIS_ROLE" >/dev/null && echo "role $POLARIS_ROLE created"
fi
aws iam put-role-policy --role-name "$POLARIS_ROLE" --policy-name read-bucket --policy-document "$(cat <<JSON
{"Version":"2012-10-17","Statement":[
 {"Effect":"Allow","Action":["s3:GetObject","s3:GetObjectVersion"],"Resource":"arn:aws:s3:::$BUCKET/*"},
 {"Effect":"Allow","Action":["s3:ListBucket","s3:GetBucketLocation"],"Resource":"arn:aws:s3:::$BUCKET"}]}
JSON
)"

# --- Access key for the Polaris user. The secret is only shown once, so write it straight into secrets.env ---
EXISTING=$(aws iam list-access-keys --user-name "$POLARIS_USER" --query 'AccessKeyMetadata[0].AccessKeyId' --output text)
if [[ "$EXISTING" != None ]]; then
  echo "access key $EXISTING already exists for $POLARIS_USER (its secret cannot be read again)."
  echo "If you lost it: aws iam delete-access-key --user-name $POLARIS_USER --access-key-id $EXISTING  and re-run this script."
else
  read -r KEY_ID KEY_SECRET <<<"$(aws iam create-access-key --user-name "$POLARIS_USER" --query 'AccessKey.[AccessKeyId,SecretAccessKey]' --output text)"
  if [[ -f secrets.env ]]; then
    setvar secrets.env POLARIS_AWS_ACCESS_KEY_ID "$KEY_ID"
    setvar secrets.env POLARIS_AWS_SECRET_ACCESS_KEY "$KEY_SECRET"
    chmod 600 secrets.env
    echo "wrote the access key of $POLARIS_USER to secrets.env"
  else
    echo "secrets.env not found. Add these lines to it NOW (the secret is shown only once):"
    echo "export POLARIS_AWS_ACCESS_KEY_ID=$KEY_ID"
    echo "export POLARIS_AWS_SECRET_ACCESS_KEY=$KEY_SECRET"
  fi
fi

echo
echo "AWS done. Role for Tableflow: arn:aws:iam::$ACCOUNT:role/$TABLEFLOW_ROLE (trust is a placeholder until 'make confluent-storage')"
