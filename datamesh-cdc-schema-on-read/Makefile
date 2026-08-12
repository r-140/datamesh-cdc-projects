.PHONY: up down reset connectors wait-connect wait-dwh init-dwh consumer consumer-log fix-grafana-perms test lint clean

# ─── Docker Compose ───────────────────────────────────────────────────

COMPOSE := docker compose

up: fix-grafana-perms
	@echo "==> Starting infrastructure (Schema-on-Read)..."
	$(COMPOSE) up -d --build
	$(MAKE) wait-connect
	$(MAKE) connectors
	$(MAKE) init-dwh
	@echo "==> Starting CDC consumer in background..."
	@mkdir -p logs
	@nohup python scripts/cdc_consumer.py > logs/consumer.log 2>&1 &
	@echo "==> Consumer PID: $$!"
	@echo "==> Logs: tail -f logs/consumer.log"

down: fix-grafana-perms
	@echo "==> Stopping infrastructure (volumes PRESERVED)..."
	$(COMPOSE) down --remove-orphans
	@echo "==> Stopping CDC consumer..."
	@-pkill -f "cdc_consumer.py" 2>/dev/null || true

reset: fix-grafana-perms
	@echo "==> FULL RESET: destroying volumes and recreating everything..."
	$(COMPOSE) down -v --remove-orphans
	-docker rm -f kafka postgres-customers postgres-orders postgres-dwh postgres-exporter prometheus grafana schema-registry kafka-connect setup-connectors 2>/dev/null || true
	@mkdir -p logs
	$(MAKE) up

# ─── DWH Initialization ───────────────────────────────────────────────

wait-dwh:
	@echo "==> Waiting for DWH (postgres-dwh) to accept connections..."
	@for i in $$(seq 1 60); do \
		if docker exec postgres-dwh pg_isready -U dwh -d datamesh_dwh >/dev/null 2>&1; then \
			echo "==> DWH is ready."; \
			exit 0; \
		fi; \
		echo "   ...attempt $$i/60, retrying in 2s"; \
		sleep 2; \
	done; \
	echo "==> ERROR: DWH did not become ready in time."; \
	exit 1

init-dwh: wait-dwh
	@echo "==> Initializing DWH raw schema (JSONB)..."
	@docker exec -i postgres-dwh psql -U dwh -d datamesh_dwh -v ON_ERROR_STOP=1 < scripts/init-dwh.sql
	@echo "==> DWH raw schema initialized."

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
	@echo "==> Registering Kafka Connect source connectors..."
	@chmod +x scripts/setup-connectors.sh
	@./scripts/setup-connectors.sh

# ─── Consumer ─────────────────────────────────────────────────────────

consumer:
	@mkdir -p logs
	@python scripts/cdc_consumer.py

consumer-log:
	@tail -f logs/consumer.log

# ─── dbt ──────────────────────────────────────────────────────────────

dbt-setup:
	cd dbt_datamesh && dbt deps

dbt-run:
	cd dbt_datamesh && dbt run

dbt-test:
	cd dbt_datamesh && dbt test

# ─── Utils ──────────────────────────────────────────────────────────────

test:
	pytest tests/ -v

lint:
	ruff check src/ tests/

fix-grafana-perms:
	@sudo chown -R $(USER):$(USER) grafana/dashboards/ 2>/dev/null || true
	@chmod 644 grafana/dashboards/*.json 2>/dev/null || true

clean: down
	-docker rm -f kafka postgres-customers postgres-orders postgres-dwh postgres-exporter prometheus grafana schema-registry kafka-connect setup-connectors 2>/dev/null || true
	-rm -f state.json /tmp/datamesh_simulate.json
	-rm -f logs/consumer.log
