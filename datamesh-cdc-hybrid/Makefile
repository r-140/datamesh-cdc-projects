.PHONY: help install test test-unit lint format up down reset status demo dbt-debug dbt-run dbt-test dbt-build dbt-docs clean

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	pip install -e ".[dev]"

test: ## Run all tests
	pytest tests/ -v --cov=src/datamesh_cdc --cov-report=term-missing

test-unit: ## Run unit tests only
	pytest tests/ -v -m "not integration"

lint: ## Run linters
	ruff check src/ tests/
	black --check src/ tests/
	mypy src/ --ignore-missing-imports

format: ## Format code
	black src/ tests/
	ruff check --fix src/ tests/

up: ## Build and start the complete hybrid pipeline
	docker compose up -d --build

down: ## Stop all services
	docker compose down

reset: ## Stop and delete demo volumes
	docker compose down -v --remove-orphans

status: ## Show service and connector status
	docker compose ps
	@curl -fsS http://localhost:8083/connectors || true

demo: ## Run additive, breaking-change, quarantine and recovery scenarios
	python scripts/run_demo.py

dbt-debug: ## Check the dbt connection and project configuration
	cd dbt_datamesh && dbt debug --profiles-dir .

dbt-run: ## Build dbt staging, Gold and governance models
	cd dbt_datamesh && dbt run --profiles-dir .

dbt-test: ## Test dbt models and warn about unresolved projections
	cd dbt_datamesh && dbt test --profiles-dir .

dbt-build: ## Run and test the complete dbt DAG
	cd dbt_datamesh && dbt build --profiles-dir .

dbt-docs: ## Generate and serve dbt lineage documentation
	cd dbt_datamesh && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .

clean: ## Clean up
	docker compose down -v
	rm -f /tmp/datamesh_state.json
	rm -rf .pytest_cache .coverage htmlcov
