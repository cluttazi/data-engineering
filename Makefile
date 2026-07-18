# lakehouse-platform — local-first demo targets.
# Everything runs without Docker by default (file transport); `make up` starts
# the optional Postgres/Redpanda/MinIO stack for the kafka path.

.DEFAULT_GOAL := help
SHELL := /bin/bash

UV ?= uv
COMPOSE := docker compose -f platform/docker/docker-compose.yml
EVENTS ?= 2000
SEED ?= 42
CORRUPT_PCT ?= 2

.PHONY: help sync up down cdc batch bronze silver export gold dbt dq rag agent report dashboard demo test lint fmt clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

sync: ## Install/refresh the Python environment (uv)
	$(UV) sync --group dev

up: ## Start optional infra (Postgres CDC source, Redpanda, MinIO)
	$(COMPOSE) up -d --wait

down: ## Stop optional infra
	$(COMPOSE) down -v

cdc: ## Generate Debezium-style CDC events (file sink by default)
	$(UV) run python -m ingestion.cdc_simulator --events $(EVENTS) --seed $(SEED) --corrupt-pct $(CORRUPT_PCT)

batch: ## Drop batch reference files (CSV/JSON/Parquet) into the landing zone
	$(UV) run python -m ingestion.batch_landing --seed $(SEED)

bronze: ## Bronze: stream CDC events into Delta (+ quarantine)
	$(UV) run python -m pipelines.bronze.job

copy-into: ## Bronze: COPY INTO-style batch load with exactly-once file ledger
	$(UV) run python -m pipelines.bronze.copy_into

silver: ## Silver: contract enforcement, dedup, SCD2 merge
	$(UV) run python -m pipelines.silver.job

export: ## Export silver snapshots to Parquet for dbt
	$(UV) run python -m pipelines.silver.export_for_dbt

gold: ## Gold: run the Scala aggregation job via spark-submit
	./scripts/run_gold.sh

dbt: ## dbt build (DuckDB) over exported silver data
	$(UV) run dbt build --project-dir transform/dbt_project --profiles-dir transform/dbt_project

dq: ## Run data-quality suites over silver/gold and print a report
	$(UV) run python -m quality.expectations.runner

rag: ## Build the RAG index over synthetic banking docs and run sample queries
	$(UV) run python -m ai.rag_pipeline --demo

agent: ## Generate an incident report from pipeline metrics + DQ results
	$(UV) run python -m ai.agents.incident_reporter

report: ## Render the static observability report (text + HTML)
	$(UV) run python -m observability.metrics.report

dashboard: ## Launch the Streamlit metrics dashboard (needs `uv sync --extra dashboard`)
	$(UV) run streamlit run observability/metrics/dashboard.py

demo: ## End-to-end: cdc -> bronze -> silver -> gold -> dbt -> dq -> rag -> agent -> summary
	$(UV) run python -m orchestration.demo

test: ## Run the Python test suite
	$(UV) run pytest

lint: ## Lint + type-check (ruff, mypy)
	$(UV) run ruff check .
	$(UV) run ruff format --check .
	$(UV) run mypy .

fmt: ## Auto-format Python code
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

clean: ## Remove all generated data (Delta tables, checkpoints, exports, warehouse)
	rm -rf data spark-warehouse metastore_db derby.log
