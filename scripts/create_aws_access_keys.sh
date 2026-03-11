#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/create_aws_access_keys.sh <iam_user_name> [env_file]

Examples:
  scripts/create_aws_access_keys.sh airflow-local-dev .env.dev
  CREATE_USER=1 scripts/create_aws_access_keys.sh airflow-local-dev .env.dev
  CREATE_USER=1 POLICY_ARN=arn:aws:iam::aws:policy/AmazonS3FullAccess \
    scripts/create_aws_access_keys.sh airflow-local-dev .env.dev

Behavior:
  - Creates a new access key for the IAM user.
  - Prints export commands for shell use.
  - If env_file exists (or is provided), upserts:
      AWS_ACCESS_KEY_ID
      AWS_SECRET_ACCESS_KEY
      AWS_SESSION_TOKEN (empty)

Optional env vars:
  CREATE_USER=1       Create IAM user if it does not exist.
  POLICY_ARN=<arn>    If CREATE_USER=1, attach this managed policy to the user.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

IAM_USER_NAME="${1:-}"
ENV_FILE="${2:-.env.dev}"
CREATE_USER="${CREATE_USER:-0}"
POLICY_ARN="${POLICY_ARN:-}"

if [[ -z "$IAM_USER_NAME" ]]; then
  echo "ERROR: Missing iam_user_name."
  usage
  exit 1
fi

if ! command -v aws >/dev/null 2>&1; then
  echo "ERROR: aws CLI not found."
  exit 1
fi

if ! aws sts get-caller-identity >/dev/null 2>&1; then
  echo "ERROR: AWS CLI is not authenticated. Run 'aws configure' or export credentials first."
  exit 1
fi

if ! aws iam get-user --user-name "$IAM_USER_NAME" >/dev/null 2>&1; then
  if [[ "$CREATE_USER" == "1" ]]; then
    echo "IAM user '$IAM_USER_NAME' not found. Creating..."
    aws iam create-user --user-name "$IAM_USER_NAME" >/dev/null

    if [[ -n "$POLICY_ARN" ]]; then
      echo "Attaching policy '$POLICY_ARN' to '$IAM_USER_NAME'..."
      aws iam attach-user-policy \
        --user-name "$IAM_USER_NAME" \
        --policy-arn "$POLICY_ARN" >/dev/null
    fi
  else
    echo "ERROR: IAM user '$IAM_USER_NAME' does not exist."
    echo "Set CREATE_USER=1 to create it."
    exit 1
  fi
fi

echo "Creating new access key for IAM user '$IAM_USER_NAME'..."
if ! ACCESS_KEY_JSON="$(aws iam create-access-key --user-name "$IAM_USER_NAME")"; then
  echo "ERROR: Failed to create access key."
  echo "If user already has 2 active keys, delete one first:"
  echo "  aws iam list-access-keys --user-name $IAM_USER_NAME"
  exit 1
fi

ACCESS_KEY_ID="$(echo "$ACCESS_KEY_JSON" | sed -n 's/.*"AccessKeyId": "\([^"]*\)".*/\1/p' | head -n1)"
SECRET_ACCESS_KEY="$(echo "$ACCESS_KEY_JSON" | sed -n 's/.*"SecretAccessKey": "\([^"]*\)".*/\1/p' | head -n1)"

if [[ -z "$ACCESS_KEY_ID" || -z "$SECRET_ACCESS_KEY" ]]; then
  echo "ERROR: Could not parse access key output."
  exit 1
fi

upsert_env_var() {
  local file="$1"
  local key="$2"
  local value="$3"

  touch "$file"
  if grep -q "^${key}=" "$file"; then
    sed -i.bak "s|^${key}=.*|${key}=${value}|" "$file"
    rm -f "${file}.bak"
  else
    printf '%s=%s\n' "$key" "$value" >> "$file"
  fi
}

upsert_env_var "$ENV_FILE" "AWS_ACCESS_KEY_ID" "$ACCESS_KEY_ID"
upsert_env_var "$ENV_FILE" "AWS_SECRET_ACCESS_KEY" "$SECRET_ACCESS_KEY"
upsert_env_var "$ENV_FILE" "AWS_SESSION_TOKEN" ""

echo
echo "Access key created successfully."
echo "IAM user: $IAM_USER_NAME"
echo "Env file updated: $ENV_FILE"
echo
echo "Export commands:"
echo "export AWS_ACCESS_KEY_ID=$ACCESS_KEY_ID"
echo "export AWS_SECRET_ACCESS_KEY=$SECRET_ACCESS_KEY"
echo "export AWS_SESSION_TOKEN="
echo
echo "Security note: this secret is shown only now. Store it safely."
