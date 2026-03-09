SHELL := /bin/bash
PYTHON ?= python3

.PHONY: env-dev env-prod bootstrap-tf-state terraform-init-dev terraform-init-prod terraform-apply-dev terraform-apply-prod install run-dev-single run-dev-all airflow-build airflow-init airflow-up airflow-down airflow-trigger-dev

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
#   make airflow-trigger-dev POSITION_ID=data_engineer START_DATE=2025-01-01 END_DATE=2025-01-31
airflow-trigger-dev:
	@if [ -z "$$POSITION_ID" ] || [ -z "$$START_DATE" ] || [ -z "$$END_DATE" ]; then \
	  echo "ERROR: Please provide POSITION_ID, START_DATE, and END_DATE, e.g."; \
	  echo "  make airflow-trigger-dev POSITION_ID=data_engineer START_DATE=2025-01-01 END_DATE=2025-01-31"; \
	  exit 1; \
	fi
	cd airflow && docker compose exec airflow-webserver \
	  airflow dags trigger latam_roles_pipeline \
	  --conf "$$(printf '{\"position_id\": \"%s\", \"start_date\": \"%s\", \"end_date\": \"%s\"}' \"$$POSITION_ID\" \"$$START_DATE\" \"$$END_DATE\")"



