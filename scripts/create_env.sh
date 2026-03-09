#!/usr/bin/env bash
set -euo pipefail

ENV_NAME="${1:-}"

if [[ -z "$ENV_NAME" ]]; then
  echo "Usage: $0 <dev|prod>"
  exit 1
fi

ENV_FILE=".env.${ENV_NAME}"

echo "This script will help you create ${ENV_FILE}."
echo "Press Enter to keep an existing value (shown in brackets)."
echo

load_existing() {
  if [[ -f "$ENV_FILE" ]]; then
    # shellcheck disable=SC2046
    export $(grep -v '^#' "$ENV_FILE" | xargs -0 -I{} echo {} | tr '\n' ' ') || true
  fi
}

prompt_var() {
  local var_name="$1"
  local prompt="$2"
  local current="${!var_name-}"

  if [[ -n "$current" ]]; then
    read -r -p "$prompt [$current]: " value || true
    if [[ -z "$value" ]]; then
      value="$current"
    fi
  else
    read -r -p "$prompt: " value || true
  fi

  export "$var_name=$value"
}

load_existing

echo "=== Gemini configuration ==="
prompt_var "GEMINI_API_KEY" "Gemini API key"

echo
echo "=== Google Custom Search configuration ==="
prompt_var "GOOGLE_CSE_API_KEY" "Google CSE API key"
prompt_var "GOOGLE_CSE_ID" "Google CSE ID (cx)"

echo
echo "=== AWS configuration ==="
prompt_var "AWS_REGION" "AWS region (e.g. us-east-1)"
prompt_var "OUTPUT_BUCKET" "S3 bucket for results (created by Terraform)"

echo
echo "=== Slack configuration (for Airflow) ==="
prompt_var "SLACK_WEBHOOK_URL" "Slack Incoming Webhook URL (optional)"
prompt_var "SLACK_CHANNEL" "Slack channel name (e.g. #data-pipelines)"

echo
echo "Writing ${ENV_FILE}..."

cat > "$ENV_FILE" <<EOF
GEMINI_API_KEY=${GEMINI_API_KEY}
GOOGLE_CSE_API_KEY=${GOOGLE_CSE_API_KEY}
GOOGLE_CSE_ID=${GOOGLE_CSE_ID}

AWS_REGION=${AWS_REGION}
OUTPUT_BUCKET=${OUTPUT_BUCKET}

SLACK_WEBHOOK_URL=${SLACK_WEBHOOK_URL}
SLACK_CHANNEL=${SLACK_CHANNEL}
EOF

echo "Done. Remember to export these variables when running locally, e.g.:"
echo "  export \$(grep -v '^#' ${ENV_FILE} | xargs)"

