.PHONY: help install test lint format up down simulate validate-silver clean

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

up: ## Start infrastructure
	docker compose up -d

down: ## Stop all services
	docker compose down -v

simulate: ## Run hybrid simulation
	python -m src.datamesh_cdc.schema_evolution_service --simulate

validate-silver: ## Validate Silver DDL against source schemas
	python -m src.datamesh_cdc.schema_evolution_service --validate-silver sql/silver/

clean: ## Clean up
	docker compose down -v
	rm -f /tmp/datamesh_state.json
	rm -rf .pytest_cache .coverage htmlcov
