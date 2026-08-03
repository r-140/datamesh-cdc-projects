"""
Schema Evolution Service — HYBRID entry point.

Bronze = JSON (never breaks).
Schema Registry = audit + detect changes + validate Silver DDL in CI.
"""

import os
import sys
import json
import time
import logging
import re
from pathlib import Path

from .schema_evolution import SchemaEvolutionManager
from .pipeline_manager import PipelineManager, SelfServeAPI
from .iceberg_sink import BronzeIcebergSink

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    logger.info("=" * 60)
    logger.info("Data Mesh CDC Platform — Hybrid (Flexible + Controlled)")
    logger.info("=" * 60)

    sr_url = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    poll_interval = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))
    state_file = os.environ.get("STATE_FILE", "/tmp/datamesh_state.json")

    manager = PipelineManager(schema_registry_url=sr_url, state_file=state_file)
    api = SelfServeAPI(manager)
    sink = BronzeIcebergSink()

    if not manager.pipelines:
        logger.info("Creating demo pipelines...")
        _create_demo_pipelines(manager)

    logger.info(f"Active pipelines: {len(manager.pipelines)}")
    for pid in manager.pipelines:
        logger.info(f"  - {pid}")

    logger.info("Starting hybrid event processing...")
    try:
        while True:
            _process_events(manager, sink)
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        sys.exit(0)


def _create_demo_pipelines(manager):
    manager.create_pipeline(
        pipeline_id="orders-to-bronze", source_topic="orders-server.public.orders",
        sink_table="bronze.orders", domain="orders",
        owner_email="orders-team@example.com", alert_webhook="http://alerts.example.com/webhook"
    )
    manager.create_pipeline(
        pipeline_id="customers-to-bronze", source_topic="customers-server.public.customers",
        sink_table="bronze.customers", domain="customers",
        owner_email="customers-team@example.com"
    )


def _process_events(manager, sink):
    for pipeline_id, pipeline in manager.pipelines.items():
        pass


def validate_silver(sql_dir: str = "sql/silver"):
    """
    CLI command to validate all Silver SQL files against source schemas.
    Usage: python -m src.datamesh_cdc.schema_evolution_service --validate-silver sql/silver/
    """
    manager = PipelineManager(schema_registry_url="http://localhost:8081", state_file=None)
    _create_demo_pipelines(manager)

    sql_path = Path(sql_dir)
    if not sql_path.exists():
        logger.error(f"SQL directory not found: {sql_dir}")
        sys.exit(1)

    all_valid = True
    for sql_file in sql_path.rglob("*.sql"):
        logger.info(f"Validating: {sql_file}")
        content = sql_file.read_text()

        # Extract field names from json_extract_scalar calls
        fields = re.findall(r"json_extract_scalar\(_payload,\s*'\$\.(\w+)'\)", content)

        # Determine pipeline from table name in CREATE VIEW
        table_match = re.search(r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+\w+\.(\w+)", content)
        if not table_match:
            logger.warning(f"Could not determine table for {sql_file}")
            continue

        table_name = table_match.group(1)
        pipeline_id = f"{table_name}-to-bronze"  # convention

        result = manager.validate_silver_for_pipeline(pipeline_id, fields)
        if result.get("valid"):
            logger.info(f"  ✅ Valid — all {len(fields)} fields found in source schema")
        else:
            all_valid = False
            logger.error(f"  ❌ Invalid — missing fields: {result.get('missing_fields')}")
            logger.error(f"     Available: {result.get('available_fields')}")

    sys.exit(0 if all_valid else 1)


def simulate():
    manager = PipelineManager(schema_registry_url="http://localhost:8081", state_file="/tmp/datamesh_simulate.json")
    sink = BronzeIcebergSink()
    _create_demo_pipelines(manager)

    event_v1 = {"id": 1, "customer_id": 100, "total_amount": 150.0, "status": "completed"}
    event_v2 = {"id": 2, "customer_id": 101, "total_amount": 299.99, "status": "pending", "promo_code": "SUMMER20"}
    event_v3 = {"id": 3, "customer_id": 102, "status": "shipped", "promo_code": "WINTER10", "discount_pct": 0.15}

    print("\n" + "=" * 70)
    print("SCENARIO: Hybrid — events with different schemas")
    print("=" * 70)

    for i, event in enumerate([event_v1, event_v2, event_v3], 1):
        print(f"\nEvent {i}: {json.dumps(event)}")
        result = sink.append_event("orders", event)
        print(f"Bronze result: {result}")

    print("\n" + "=" * 70)
    print("All events appended successfully — pipeline never breaks!")
    print("=" * 70)

    print("\nNow validate Silver DDL:")
    print("  python -m src.datamesh_cdc.schema_evolution_service --validate-silver sql/silver/")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--simulate":
        simulate()
    elif len(sys.argv) > 1 and sys.argv[1] == "--validate-silver":
        sql_dir = sys.argv[2] if len(sys.argv) > 2 else "sql/silver"
        validate_silver(sql_dir)
    else:
        main()
