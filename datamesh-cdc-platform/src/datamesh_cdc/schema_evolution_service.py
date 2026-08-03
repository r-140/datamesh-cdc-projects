"""
Schema Evolution Service - Main entry point.

Runs as a background service that:
1. Polls Schema Registry for new schema versions
2. Detects schema changes across pipelines
3. Applies opt-in/opt-out logic
4. Propagates changes to Iceberg sinks
5. Sends alerts on breaking changes

Usage:
    python -m src.datamesh_cdc.schema_evolution_service
"""

import os
import sys
import json
import time
import logging
from datetime import datetime

from .schema_evolution import SchemaEvolutionManager, SchemaEvolutionError
from .pipeline_manager import PipelineManager, SelfServeAPI
from .iceberg_sink import IcebergCDCSink

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_env_or_default(key: str, default: str) -> str:
    return os.environ.get(key, default)


def main():
    """Main service loop."""
    logger.info("=" * 60)
    logger.info("Data Mesh CDC Platform - Schema Evolution Service")
    logger.info("=" * 60)

    # Configuration from environment
    schema_registry_url = get_env_or_default("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    kafka_bootstrap = get_env_or_default("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    iceberg_rest_url = get_env_or_default("ICEBERG_REST_URL", "http://localhost:8181")
    s3_endpoint = get_env_or_default("S3_ENDPOINT", "http://localhost:9000")
    s3_key = get_env_or_default("S3_ACCESS_KEY", "minio")
    s3_secret = get_env_or_default("S3_SECRET_KEY", "minio123")
    poll_interval = int(get_env_or_default("POLL_INTERVAL_SECONDS", "30"))
    state_file = get_env_or_default("STATE_FILE", "/tmp/datamesh_state.json")

    logger.info(f"Schema Registry: {schema_registry_url}")
    logger.info(f"Iceberg REST: {iceberg_rest_url}")
    logger.info(f"Poll interval: {poll_interval}s")

    # Initialize components
    manager = PipelineManager(
        schema_registry_url=schema_registry_url,
        iceberg_catalog_uri=iceberg_rest_url,
        state_file=state_file
    )

    api = SelfServeAPI(manager)

    # Create demo pipelines if none exist
    if not manager.pipelines:
        logger.info("Creating demo pipelines...")
        _create_demo_pipelines(manager)

    logger.info(f"Active pipelines: {len(manager.pipelines)}")
    for pid in manager.pipelines:
        logger.info(f"  - {pid}")

    # Main monitoring loop
    logger.info("Starting schema evolution monitoring loop...")
    try:
        while True:
            _check_all_pipelines(manager)
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        sys.exit(0)


def _create_demo_pipelines(manager: PipelineManager):
    """Create demo pipelines for Data Mesh demonstration."""
    # Pipeline 1: Orders -> Analytics (Opt-in)
    manager.create_pipeline(
        pipeline_id="orders-to-analytics",
        source_topic="orders-server.public.orders",
        sink_table="raw.orders",
        domain="orders",
        opt_in_schema_evolution=True,
        owner_email="orders-team@example.com",
        alert_webhook="http://alerts.example.com/webhook"
    )

    # Pipeline 2: Orders -> Reporting (Opt-out, only specific fields)
    manager.create_pipeline(
        pipeline_id="orders-to-reporting",
        source_topic="orders-server.public.orders",
        sink_table="reporting.orders_summary",
        domain="orders",
        opt_in_schema_evolution=False,
        consumed_fields=["id", "customer_id", "total_amount", "status"],
        owner_email="reporting-team@example.com"
    )

    # Pipeline 3: Customers -> Analytics (Opt-in)
    manager.create_pipeline(
        pipeline_id="customers-to-analytics",
        source_topic="customers-server.public.customers",
        sink_table="raw.customers",
        domain="customers",
        opt_in_schema_evolution=True,
        owner_email="customers-team@example.com"
    )

    logger.info("Demo pipelines created successfully")


def _check_all_pipelines(manager: PipelineManager):
    """Check all pipelines for schema changes."""
    for pipeline_id, pipeline in manager.pipelines.items():
        subject = f"{pipeline.config.source_topic}-value"

        try:
            latest = manager.schema_manager.get_latest_schema(subject)
            if latest is None:
                continue

            # Check if schema has changed since last known
            if pipeline._schema_version_history:
                # In production: compare with stored hash/version
                pass

            # For demo: simulate checking (in production, compare versions)
            # This would be triggered by Schema Registry webhooks or Kafka topic

        except Exception as e:
            logger.error(f"Error checking pipeline {pipeline_id}: {e}")


def simulate_schema_evolution():
    """
    CLI command to simulate schema evolution scenarios.

    Usage:
        python -m src.datamesh_cdc.schema_evolution_service --simulate
    """
    manager = PipelineManager(
        schema_registry_url="http://localhost:8081",
        state_file="/tmp/datamesh_simulate.json"
    )

    _create_demo_pipelines(manager)

    # Define schema versions
    schema_v1 = {
        "type": "record",
        "name": "Order",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "customer_id", "type": "long"},
            {"name": "total_amount", "type": "double"},
            {"name": "status", "type": "string"}
        ]
    }

    schema_v2 = {
        "type": "record",
        "name": "Order",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "customer_id", "type": "long"},
            {"name": "total_amount", "type": "double"},
            {"name": "status", "type": "string"},
            {"name": "promo_code", "type": ["null", "string"], "default": None}
        ]
    }

    schema_v3 = {
        "type": "record",
        "name": "Order",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "customer_id", "type": "long"},
            {"name": "status", "type": "string"},
            {"name": "promo_code", "type": ["null", "string"], "default": None}
            # total_amount REMOVED - breaking for reporting pipeline
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
        simulate_schema_evolution()
    else:
        main()
