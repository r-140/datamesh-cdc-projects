.PHONY: up down fix-grafana-perms connectors dbt-setup dbt-run dbt-test simulate test lint clean

fix-grafana-perms:
	@echo "Fixing Grafana dashboard permissions..."
	@sudo chown -R $(USER):$(USER) grafana/dashboards/ 2>/dev/null || true
	@chmod 644 grafana/dashboards/*.json 2>/dev/null || true

up: docker compose down -v --remove-orphans
	docker rm -f minio kafka postgres-customers postgres-orders prometheus grafana trino iceberg-rest schema-registry kafka-connect schema-evolution-service 2>/dev/null || true
	docker compose up -d --build
	@echo "Waiting for services..."
	@sleep 25
	@./scripts/setup-connectors.sh || true

down: fix-grafana-perms
	docker compose down -v --remove-orphans

connectors:
	./scripts/setup-connectors.sh

dbt-setup:
	cd dbt_datamesh && dbt deps

dbt-run:
	cd dbt_datamesh && dbt run

dbt-test:
	cd dbt_datamesh && dbt test

simulate:
	rm -f state.json /tmp/datamesh_simulate.json
	python -m src.datamesh_cdc.schema_evolution_service --simulate

test:
	pytest tests/ -v

lint:
	ruff check src/ tests/

clean:
	docker compose down -v --remove-orphans
	docker rm -f minio kafka postgres-customers postgres-orders prometheus grafana trino iceberg-rest schema-registry kafka-connect schema-evolution-service 2>/dev/null || true
	rm -f state.json /tmp/datamesh_simulate.json