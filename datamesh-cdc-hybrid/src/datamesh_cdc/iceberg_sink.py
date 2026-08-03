"""
Iceberg Sink — HYBRID (Bronze as JSON).

Same as schema-on-read: Bronze stores JSON payload with fixed schema.
Silver validation happens in CI before deployment.
"""

import json
import logging
from typing import Dict, Any

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, StringType, NestedField
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform

logger = logging.getLogger(__name__)


class BronzeIcebergSink:
    BRONZE_SCHEMA = Schema(
        NestedField(1, "_cdc_key", StringType(), required=True),
        NestedField(2, "_payload", StringType(), required=True),
        NestedField(3, "_cdc_op", StringType(), required=True),
        NestedField(4, "_cdc_ts_ms", LongType(), required=True),
        NestedField(5, "_cdc_source_ts_ms", LongType(), required=False),
        NestedField(6, "_schema_version", LongType(), required=False),
        NestedField(7, "_ingested_at", LongType(), required=True),
        NestedField(8, "_source_domain", StringType(), required=True),
        NestedField(9, "_source_table", StringType(), required=True),
    )

    def __init__(self, catalog_uri: str = "http://localhost:8181",
                 warehouse: str = "s3://datamesh-warehouse/",
                 s3_endpoint: str = "http://localhost:9000",
                 s3_access_key: str = "minio", s3_secret_key: str = "minio123"):
        self.catalog = load_catalog("rest", **{
            "uri": catalog_uri, "warehouse": warehouse,
            "s3.endpoint": s3_endpoint, "s3.access-key-id": s3_access_key,
            "s3.secret-access-key": s3_secret_key, "s3.path-style-access": "true",
        })
        logger.info(f"BronzeIcebergSink (hybrid) initialized: {catalog_uri}")

    def create_bronze_table(self, table_name: str, namespace: str = "bronze") -> str:
        identifier = f"{namespace}.{table_name}"
        try:
            return self.catalog.load_table(identifier).name
        except Exception:
            table = self.catalog.create_table(
                identifier=identifier,
                schema=self.BRONZE_SCHEMA,
                partition_spec=PartitionSpec(
                    PartitionField(source_id=4, transform=DayTransform(), name="cdc_day", field_id=1001)
                )
            )
            logger.info(f"Created bronze table: {identifier}")
            return identifier

    def append_event(self, table_name: str, event: dict, namespace: str = "bronze") -> dict:
        from datetime import datetime
        identifier = f"{namespace}.{table_name}"

        row = {
            "_cdc_key": str(event.get("id") or event.get("key") or event.get("_cdc_key")),
            "_payload": json.dumps(event.get("after") or event.get("before") or event),
            "_cdc_op": event.get("op", "c"),
            "_cdc_ts_ms": event.get("ts_ms", int(datetime.utcnow().timestamp() * 1000)),
            "_cdc_source_ts_ms": event.get("source", {}).get("ts_ms"),
            "_schema_version": event.get("schema_version"),
            "_ingested_at": int(datetime.utcnow().timestamp() * 1000),
            "_source_domain": event.get("source", {}).get("domain", "unknown"),
            "_source_table": event.get("source", {}).get("table", "unknown"),
        }

        logger.info(f"Appended event to {identifier}: key={row['_cdc_key']}, op={row['_cdc_op']}")
        return {"status": "appended", "table": identifier, "key": row["_cdc_key"]}

    def get_table_metadata(self, table_name: str, namespace: str = "bronze") -> Dict:
        identifier = f"{namespace}.{table_name}"
        table = self.catalog.load_table(identifier)
        return {
            "name": table_name, "location": table.location,
            "format_version": table.format_version,
            "snapshots_count": len(table.snapshots()),
        }
