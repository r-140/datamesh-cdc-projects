.PHONY: up down reset connectors wait-connect fix-grafana-perms dbt-setup dbt-run dbt-test simulate test lint clean

# ─── Docker Compose ───────────────────────────────────────────────────

COMPOSE := docker compose

up: fix-grafana-perms
	@echo "==> Starting infrastructure (persistent mode)..."
	$(COMPOSE) up -d --build
	$(MAKE) wait-connect
	$(MAKE) connectors

down: fix-grafana-perms
	@echo "==> Stopping infrastructure (volumes PRESERVED)..."
	$(COMPOSE) down --remove-orphans

reset: fix-grafana-perms
	@echo "==> FULL RESET: destroying volumes and recreating everything..."
	$(COMPOSE) down -v --remove-orphans
	-docker rm -f kafka postgres-customers postgres-orders postgres-dwh prometheus grafana schema-registry kafka-connect setup-connectors 2>/dev/null || true
	$(MAKE) up

# ─── Connectors ───────────────────────────────────────────────────────

wait-connect:
	@echo "==> Waiting for Kafka Connect REST API on :8083 ..."
	@for i in $$(seq 1 60); do \
		if curl -sf http://localhost:8083/connectors >/dev/null 2>&1; then \
			echo "==> Kafka Connect is ready."; \
			exit 0; \
		fi; \
		echo "   ...attempt $$i/60, retrying in 2s"; \
		sleep 2; \
	done; \
	echo "==> ERROR: Kafka Connect did not become ready in time."; \
	exit 1

connectors:
	@echo "==> Registering Kafka Connect connectors..."
	@chmod +x scripts/setup-connectors.sh
	@./scripts/setup-connectors.sh

# ─── dbt ──────────────────────────────────────────────────────────────

dbt-setup:
	cd dbt_datamesh && dbt deps

dbt-run:
	cd dbt_datamesh && dbt run

dbt-test:
	cd dbt_datamesh && dbt test

# ─── Utils ──────────────────────────────────────────────────────────────

simulate:
	rm -f state.json /tmp/datamesh_simulate.json
	python -m src.datamesh_cdc.schema_evolution_service --simulate

test:
	pytest tests/ -v

lint:
	ruff check src/ tests/

fix-grafana-perms:
	@sudo chown -R $(USER):$(USER) grafana/dashboards/ 2>/dev/null || true
	@chmod 644 grafana/dashboards/*.json 2>/dev/null || true

clean: down
	-docker rm -f kafka postgres-customers postgres-orders postgres-dwh prometheus grafana schema-registry kafka-connect setup-connectors 2>/dev/null || true
	rm -f state.json /tmp/datamesh_simulate.json
