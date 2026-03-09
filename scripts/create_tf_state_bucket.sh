#!/usr/bin/env bash
set -euo pipefail

BUCKET="${TF_STATE_BUCKET:-latam-roles-tf-state}"
REGION="${TF_STATE_REGION:-us-east-1}"

echo "Ensuring Terraform state bucket exists: ${BUCKET} (${REGION})"

if aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  echo "Bucket ${BUCKET} already exists."
  exit 0
fi

echo "Bucket ${BUCKET} not found, creating..."

if [ "$REGION" = "us-east-1" ]; then
  aws s3api create-bucket \
    --bucket "$BUCKET" \
    --region "$REGION"
else
  aws s3api create-bucket \
    --bucket "$BUCKET" \
    --region "$REGION" \
    --create-bucket-configuration LocationConstraint="$REGION"
fi

echo "Bucket ${BUCKET} created (or now available)."

