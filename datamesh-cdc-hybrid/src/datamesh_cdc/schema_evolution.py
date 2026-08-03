"""
Schema Evolution Manager — HYBRID.

Combines flexibility of schema-on-read with control of schema-on-write:
- Bronze stores JSON (never breaks)
- Schema Registry detects changes and notifies
- CI validates Silver DDL against source schema before deployment
"""

import json
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Any, Set
from datetime import datetime

import requests
from confluent_kafka.schema_registry import SchemaRegistryClient

logger = logging.getLogger(__name__)


class SchemaChangeType(Enum):
    FIELD_ADDED = "FIELD_ADDED"
    FIELD_REMOVED = "FIELD_REMOVED"
    FIELD_MODIFIED = "FIELD_MODIFIED"
    TYPE_CHANGED = "TYPE_CHANGED"


@dataclass
class SchemaChange:
    change_type: SchemaChangeType
    field_name: str
    old_value: Optional[Any] = None
    new_value: Optional[Any] = None


@dataclass
class PipelineConfig:
    pipeline_id: str
    source_topic: str
    sink_table: str
    domain: str
    owner_email: str = ""
    alert_webhook: Optional[str] = None
    track_schema_history: bool = True


class SchemaEvolutionManager:
    """
    Hybrid schema manager:
    - Detects changes for audit/notification (non-blocking)
    - Validates Silver DDL against source schema (for CI)
    """

    def __init__(self, schema_registry_url: str):
        self.sr_client = SchemaRegistryClient({'url': schema_registry_url})
        self.base_url = schema_registry_url.rstrip('/')
        logger.info(f"SchemaEvolutionManager (hybrid) initialized: {schema_registry_url}")

    def register_schema(self, subject: str, schema: dict) -> int:
        schema_str = json.dumps(schema)
        try:
            schema_id = self.sr_client.register_schema(subject, schema_str)
            logger.info(f"Schema registered for audit: {subject} -> ID {schema_id}")
            return schema_id
        except Exception as e:
            logger.warning(f"Schema registration failed (non-blocking): {e}")
            return -1

    def get_latest_schema(self, subject: str) -> Optional[dict]:
        try:
            version = self.sr_client.get_latest_version(subject)
            return json.loads(version.schema.schema_str)
        except Exception:
            return None

    def detect_schema_changes(self, old_schema: dict, new_schema: dict) -> List[SchemaChange]:
        changes = []
        old_fields = {f['name']: f for f in old_schema.get('fields', [])}
        new_fields = {f['name']: f for f in new_schema.get('fields', [])}

        for name, field in old_fields.items():
            if name not in new_fields:
                changes.append(SchemaChange(
                    change_type=SchemaChangeType.FIELD_REMOVED,
                    field_name=name,
                    old_value=field
                ))

        for name, field in new_fields.items():
            if name not in old_fields:
                changes.append(SchemaChange(
                    change_type=SchemaChangeType.FIELD_ADDED,
                    field_name=name,
                    new_value=field
                ))

        for name in old_fields:
            if name in new_fields and old_fields[name] != new_fields[name]:
                changes.append(SchemaChange(
                    change_type=SchemaChangeType.FIELD_MODIFIED,
                    field_name=name,
                    old_value=old_fields[name],
                    new_value=new_fields[name]
                ))

        return changes

    def notify_changes(self, subject: str, changes: List[SchemaChange], webhook: Optional[str] = None):
        if not changes:
            return
        logger.info(f"Detected {len(changes)} schema changes for {subject}")
        for c in changes:
            logger.info(f"  {c.change_type.value}: {c.field_name}")

        if webhook:
            payload = {
                "subject": subject,
                "changes": [{"type": c.change_type.value, "field": c.field_name} for c in changes],
                "timestamp": datetime.utcnow().isoformat(),
                "action": "NOTIFIED"
            }
            try:
                requests.post(webhook, json=payload, timeout=10)
            except Exception as e:
                logger.warning(f"Webhook failed (non-blocking): {e}")

    def validate_silver_ddl(self, subject: str, expected_fields: List[str]) -> dict:
        """
        Validate that all fields required by Silver DDL exist in the source schema.
        Called in CI before deploying Silver views.
        """
        latest = self.get_latest_schema(subject)
        if not latest:
            return {"valid": False, "error": "No schema found in registry", "missing_fields": expected_fields}

        available: Set[str] = {f['name'] for f in latest.get('fields', [])}
        missing = [f for f in expected_fields if f not in available]

        return {
            "valid": len(missing) == 0,
            "subject": subject,
            "missing_fields": missing,
            "available_fields": sorted(list(available)),
            "expected_fields": expected_fields
        }


class DataMeshPipeline:
    """Hybrid pipeline: Bronze = JSON (flexible), but schema tracked for validation."""

    def __init__(self, config: PipelineConfig, schema_manager: SchemaEvolutionManager):
        self.config = config
        self.schema_manager = schema_manager
        self.subject = f"{config.source_topic}-value"
        self._state = "RUNNING"
        self._schema_version_history: List[dict] = []

    @property
    def state(self) -> str:
        return self._state

    def handle_event(self, event: dict, current_schema: Optional[dict] = None) -> dict:
        logger.info(f"[{self.config.pipeline_id}] Processing event")

        result = {"action": "APPENDED", "pipeline_id": self.config.pipeline_id}

        if current_schema and self.config.track_schema_history:
            latest = self.schema_manager.get_latest_schema(self.subject)
            if latest and latest != current_schema:
                changes = self.schema_manager.detect_schema_changes(latest, current_schema)
                self.schema_manager.notify_changes(self.subject, changes, self.config.alert_webhook)
                self.schema_manager.register_schema(self.subject, current_schema)

                self._schema_version_history.append({
                    "timestamp": datetime.utcnow().isoformat(),
                    "changes": [c.__dict__ for c in changes],
                    "action": "NOTIFIED"
                })
                result["schema_changes_detected"] = len(changes)

        return result

    def get_schema_history(self) -> List[dict]:
        return self._schema_version_history
