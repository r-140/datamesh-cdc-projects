.PHONY: help install test lint format up down logs simulate clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	pip install -e ".[dev]"

test: ## Run all tests
	pytest tests/ -v --cov=src/datamesh_cdc --cov-report=term-missing

test-unit: ## Run unit tests only
	pytest tests/ -v -m "not integration"

test-integration: ## Run integration tests
	pytest tests/ -v -m integration

lint: ## Run linters
	ruff check src/ tests/
	black --check src/ tests/
	mypy src/ --ignore-missing-imports

format: ## Format code
	black src/ tests/
	ruff check --fix src/ tests/

up: ## Start all services
	docker compose up -d

down: ## Stop all services
	docker compose down -v

logs: ## Show logs
	docker compose logs -f

simulate: ## Run schema evolution simulation
	python -m src.datamesh_cdc.schema_evolution_service --simulate

register-connectors: ## Register Debezium connectors
	bash scripts/register_connectors.sh

demo-schema-evolution: ## Run schema evolution demo
	bash -c 'echo "Run these commands in psql:" && cat scripts/schema_evolution_demo.sql'

psql-orders: ## Connect to orders database
	psql -h localhost -p 5432 -U postgres -d orders_db

psql-customers: ## Connect to customers database
	psql -h localhost -p 5433 -U postgres -d customers_db

trino: ## Connect to Trino
	trino --server localhost:8080 --catalog iceberg --schema raw

clean: ## Clean up volumes and state
	docker compose down -v
	rm -f /tmp/datamesh_state.json
	rm -rf .pytest_cache .coverage htmlcov
