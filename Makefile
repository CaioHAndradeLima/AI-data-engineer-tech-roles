SHELL := /bin/bash
PYTHON ?= python3

.PHONY: env-dev env-prod bootstrap-tf-state terraform-init-dev terraform-init-prod terraform-apply-dev terraform-apply-prod install run-dev-single run-dev-all airflow-build airflow-init airflow-up airflow-down airflow-trigger-dev airflow-trigger-all-dev aws-create-keys-dev aws-create-keys-prod streamlit-install streamlit-run

env-dev:
	@chmod +x scripts/create_env.sh
	@./scripts/create_env.sh dev

env-prod:
	@chmod +x scripts/create_env.sh
	@./scripts/create_env.sh prod

bootstrap-tf-state:
	@chmod +x scripts/create_tf_state_bucket.sh
	@./scripts/create_tf_state_bucket.sh

terraform-init-dev: bootstrap-tf-state
	cd infra/terraform && terraform init -backend-config="key=latam-roles/dev.tfstate"

terraform-init-prod: bootstrap-tf-state
	cd infra/terraform && terraform init -backend-config="key=latam-roles/prod.tfstate"

terraform-apply-dev:
	cd infra/terraform && terraform apply -var="environment=dev" -var-file="dev.tfvars"

terraform-apply-prod:
	cd infra/terraform && terraform apply -var="environment=prod" -var-file="prod.tfvars"

install:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt

# Run pipeline locally for a single position using .env.dev
# Usage example:
#   make run-dev-single POSITION="Data Engineer" START_DATE=2025-01-01 END_DATE=2025-01-31
run-dev-single: install
	@if [ -z "$$START_DATE" ] || [ -z "$$END_DATE" ]; then \
	  echo "ERROR: Please provide START_DATE and END_DATE, e.g."; \
	  echo "  make run-dev-single POSITION=\"Data Engineer\" START_DATE=2025-01-01 END_DATE=2025-01-31"; \
	  exit 1; \
	fi
	@export $$(grep -v '^#' .env.dev | xargs) && \
	  $(PYTHON) pipeline.py \
	    --position "$${POSITION:-Data Engineer}" \
	    --start-date "$$START_DATE" \
	    --end-date "$$END_DATE" \
	    --mode ai_only \
	    --no-s3

# Run pipeline locally for all positions in positions.json using .env.dev
# Usage example:
#   make run-dev-all START_DATE=2025-01-01 END_DATE=2025-01-31
run-dev-all: install
	@if [ -z "$$START_DATE" ] || [ -z "$$END_DATE" ]; then \
	  echo "ERROR: Please provide START_DATE and END_DATE, e.g."; \
	  echo "  make run-dev-all START_DATE=2025-01-01 END_DATE=2025-01-31"; \
	  exit 1; \
	fi
	@export $$(grep -v '^#' .env.dev | xargs) && \
	  $(PYTHON) pipeline.py \
	    --start-date "$$START_DATE" \
	    --end-date "$$END_DATE" \
	    --positions-config positions.json \
	    --mode ai_only \
	    --no-s3

airflow-build:
	cd airflow && docker compose build

airflow-init:
	cd airflow && docker compose up airflow-init

airflow-up:
	cd airflow && docker compose up -d airflow-webserver airflow-scheduler

airflow-down:
	cd airflow && docker compose down

# Trigger the Airflow DAG in the local docker-compose environment.
# Usage:
#   make airflow-trigger-dev POSITION_NAME="Data Engineer" YEAR=2026 MONTH=2
airflow-trigger-dev:
	@if [ -z "$$POSITION_NAME" ] || [ -z "$$YEAR" ] || [ -z "$$MONTH" ]; then \
	  echo "ERROR: Please provide POSITION_NAME, YEAR, and MONTH, e.g."; \
	  echo "  make airflow-trigger-dev POSITION_NAME=\"Data Engineer\" YEAR=2026 MONTH=2"; \
	  exit 1; \
	fi
	cd airflow && docker compose exec airflow-webserver \
	  airflow dags trigger latam_roles_pipeline \
	  --conf "$$(printf '{"position_name":"%s","year":"%s","month":"%s"}' "$$POSITION_NAME" "$$YEAR" "$$MONTH")"

# Trigger the Airflow DAG for all role groups in positions.json for a specific month.
# Usage:
#   make airflow-trigger-all-dev YEAR=2026 MONTH=2
airflow-trigger-all-dev:
	@if [ -z "$$YEAR" ] || [ -z "$$MONTH" ]; then \
	  echo "ERROR: Please provide YEAR and MONTH, e.g."; \
	  echo "  make airflow-trigger-all-dev YEAR=2026 MONTH=2"; \
	  exit 1; \
	fi
	cd airflow && docker compose exec airflow-webserver \
	  airflow dags trigger latam_roles_pipeline \
	  --conf "$$(printf '{"year":"%s","month":"%s"}' "$$YEAR" "$$MONTH")"

# Create IAM access keys via AWS CLI and write them into .env.dev
# Usage:
#   make aws-create-keys-dev IAM_USER=airflow-local-dev
#   make aws-create-keys-dev IAM_USER=airflow-local-dev CREATE_USER=1 POLICY_ARN=arn:aws:iam::aws:policy/AmazonS3FullAccess
aws-create-keys-dev:
	@if [ -z "$$IAM_USER" ]; then \
	  echo "ERROR: Please provide IAM_USER, e.g."; \
	  echo "  make aws-create-keys-dev IAM_USER=airflow-local-dev"; \
	  exit 1; \
	fi
	@chmod +x scripts/create_aws_access_keys.sh
	@CREATE_USER="$${CREATE_USER:-0}" POLICY_ARN="$${POLICY_ARN:-}" \
	  ./scripts/create_aws_access_keys.sh "$$IAM_USER" ".env.dev"

# Same helper for .env.prod
aws-create-keys-prod:
	@if [ -z "$$IAM_USER" ]; then \
	  echo "ERROR: Please provide IAM_USER, e.g."; \
	  echo "  make aws-create-keys-prod IAM_USER=airflow-prod-user"; \
	  exit 1; \
	fi
	@chmod +x scripts/create_aws_access_keys.sh
	@CREATE_USER="$${CREATE_USER:-0}" POLICY_ARN="$${POLICY_ARN:-}" \
	  ./scripts/create_aws_access_keys.sh "$$IAM_USER" ".env.prod"

streamlit-install:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r streamlit/requirements.txt

streamlit-run: streamlit-install
	@export $$(grep -v '^#' .env.dev | xargs) && \
	  STREAMLIT_ENV_FILE=.env.dev $(PYTHON) -m streamlit run streamlit/app.py
