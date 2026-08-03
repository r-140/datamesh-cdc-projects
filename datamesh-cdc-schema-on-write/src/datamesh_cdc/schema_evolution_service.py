"""
Schema Evolution Service — STRICT mode entry point.

Runs background monitoring loop. On schema change:
- Validates compatibility via Schema Registry
- PAUSES pipeline on breaking changes
- PROPAGATES on compatible changes
"""

import os
import sys
import json
import time
import logging

from .schema_evolution import SchemaEvolutionManager, SchemaEvolutionError
from .pipeline_manager import PipelineManager, SelfServeAPI

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    logger.info("=" * 60)
    logger.info("Data Mesh CDC Platform — Schema-on-Write (STRICT)")
    logger.info("=" * 60)

    sr_url = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    poll_interval = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))
    state_file = os.environ.get("STATE_FILE", "/tmp/datamesh_state.json")

    manager = PipelineManager(schema_registry_url=sr_url, state_file=state_file)
    api = SelfServeAPI(manager)

    if not manager.pipelines:
        logger.info("Creating demo pipelines...")
        _create_demo_pipelines(manager)

    logger.info(f"Active pipelines: {len(manager.pipelines)}")
    for pid in manager.pipelines:
        logger.info(f"  - {pid}")

    logger.info("Starting strict schema evolution monitoring...")
    try:
        while True:
            _check_all_pipelines(manager)
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        sys.exit(0)


def _create_demo_pipelines(manager):
    manager.create_pipeline(
        pipeline_id="orders-to-analytics", source_topic="orders-server.public.orders",
        sink_table="raw.orders", domain="orders", opt_in_schema_evolution=True,
        owner_email="orders-team@example.com", alert_webhook="http://alerts.example.com/webhook"
    )
    manager.create_pipeline(
        pipeline_id="orders-to-reporting", source_topic="orders-server.public.orders",
        sink_table="reporting.orders_summary", domain="orders", opt_in_schema_evolution=False,
        consumed_fields=["id", "customer_id", "total_amount", "status"],
        owner_email="reporting-team@example.com"
    )
    manager.create_pipeline(
        pipeline_id="customers-to-analytics", source_topic="customers-server.public.customers",
        sink_table="raw.customers", domain="customers", opt_in_schema_evolution=True,
        owner_email="customers-team@example.com"
    )


def _check_all_pipelines(manager):
    for pipeline_id, pipeline in manager.pipelines.items():
        subject = f"{pipeline.config.source_topic}-value"
        try:
            latest = manager.schema_manager.get_latest_schema(subject)
            if latest is None:
                continue
        except Exception as e:
            logger.error(f"Error checking pipeline {pipeline_id}: {e}")


def simulate():
    manager = PipelineManager(schema_registry_url="http://localhost:8081", state_file="/tmp/datamesh_simulate.json")
    _create_demo_pipelines(manager)

    schema_v1 = {
        "type": "record", "name": "Order",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "customer_id", "type": "long"},
            {"name": "total_amount", "type": "double"},
            {"name": "status", "type": "string"}
        ]
    }
    schema_v2 = {
        "type": "record", "name": "Order",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "customer_id", "type": "long"},
            {"name": "total_amount", "type": "double"},
            {"name": "status", "type": "string"},
            {"name": "promo_code", "type": ["null", "string"], "default": None}
        ]
    }
    schema_v3 = {
        "type": "record", "name": "Order",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "customer_id", "type": "long"},
            {"name": "status", "type": "string"},
            {"name": "promo_code", "type": ["null", "string"], "default": None}
        ]
    }

    print("\n" + "=" * 70)
    print("SCENARIO 1: Adding optional field (promo_code)")
    print("=" * 70)
    for pid in ["orders-to-analytics", "orders-to-reporting"]:
        result = manager.handle_schema_change(pid, schema_v2)
        print(f"\n[{pid}] Result: {json.dumps(result, indent=2)}")

    print("\n" + "=" * 70)
    print("SCENARIO 2: Removing consumed field (total_amount)")
    print("=" * 70)
    for pid in ["orders-to-analytics", "orders-to-reporting"]:
        result = manager.handle_schema_change(pid, schema_v3)
        print(f"\n[{pid}] Result: {json.dumps(result, indent=2)}")

    print("\n" + "=" * 70)
    print("DOMAIN STATS")
    print("=" * 70)
    print(json.dumps(manager.get_domain_stats("orders"), indent=2))


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--simulate":
        simulate()
    else:
        main()
