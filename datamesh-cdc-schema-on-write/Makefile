.PHONY: up down simulate test lint clean

up: ## Start infrastructure (cleans old containers)
	docker compose down -v --remove-orphans
	docker rm -f minio kafka postgres-customers postgres-orders prometheus grafana trino iceberg-rest schema-registry kafka-connect schema-evolution-service 2>/dev/null || true
	docker compose up -d --build
	@echo "Waiting for services..."
	@sleep 25
	@./scripts/setup-connectors.sh || true

down: ## Stop infrastructure
	docker compose down -v --remove-orphans

connectors: ## Register/update Debezium connectors
	./scripts/setup-connectors.sh

simulate: ## Run schema evolution simulation
	rm -f state.json /tmp/datamesh_simulate.json
	python -m src.datamesh_cdc.schema_evolution_service --simulate

test: ## Run pytest
	pytest tests/ -v

lint: ## Run ruff
	ruff check src/ tests/

clean: ## Remove all containers, volumes, and state
	docker compose down -v --remove-orphans
	docker rm -f minio kafka postgres-customers postgres-orders prometheus grafana trino iceberg-rest schema-registry kafka-connect schema-evolution-service 2>/dev/null || true
	rm -f state.json /tmp/datamesh_simulate.json