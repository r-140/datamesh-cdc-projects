"""
Schema Evolution Manager for Data Mesh CDC Platform.

Implements concepts from Netflix Data Mesh:
- Backward/Forward/Full compatibility checks
- Consumer Schema with opt-in/opt-out evolution
- Schema propagation to downstream sinks
"""

import json
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set, Any
from datetime import datetime

import requests
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError

logger = logging.getLogger(__name__)


class CompatibilityLevel(Enum):
    BACKWARD = "BACKWARD"
    FORWARD = "FORWARD"
    FULL = "FULL"
    NONE = "NONE"


class SchemaChangeType(Enum):
    FIELD_ADDED = "FIELD_ADDED"
    FIELD_REMOVED = "FIELD_REMOVED"
    FIELD_MODIFIED = "FIELD_MODIFIED"
    TYPE_CHANGED = "TYPE_CHANGED"
    DEFAULT_CHANGED = "DEFAULT_CHANGED"


@dataclass
class SchemaChange:
    change_type: SchemaChangeType
    field_name: str
    old_value: Optional[Any] = None
    new_value: Optional[Any] = None
    is_breaking: bool = False


@dataclass
class PipelineConfig:
    pipeline_id: str
    source_topic: str
    sink_table: str
    domain: str
    opt_in_schema_evolution: bool = True
    consumed_fields: List[str] = field(default_factory=list)
    compatibility_level: CompatibilityLevel = CompatibilityLevel.BACKWARD
    owner_email: str = ""
    alert_webhook: Optional[str] = None


class SchemaEvolutionManager:
    """Manages schema registration, compatibility checks and evolution."""

    def __init__(self, schema_registry_url: str):
        self.sr_client = SchemaRegistryClient({'url': schema_registry_url})
        self.base_url = schema_registry_url.rstrip('/')
        logger.info(f"SchemaEvolutionManager initialized: {schema_registry_url}")

    def register_schema(self, subject: str, schema: dict) -> int:
        """Register new schema version with compatibility check."""
        schema_str = json.dumps(schema)
        try:
            schema_id = self.sr_client.register_schema(subject, schema_str)
            logger.info(f"Schema registered: {subject} -> ID {schema_id}")
            return schema_id
        except SchemaRegistryError as e:
            logger.error(f"Schema registration failed for {subject}: {e}")
            raise SchemaEvolutionError(f"Incompatible schema change: {e}") from e

    def get_latest_schema(self, subject: str) -> Optional[dict]:
        """Get latest registered schema for subject."""
        try:
            version = self.sr_client.get_latest_version(subject)
            return json.loads(version.schema.schema_str)
        except SchemaRegistryError:
            return None

    def get_schema_changes(self, subject: str, old_version: int, new_version: int) -> List[SchemaChange]:
        """Analyze diff between two schema versions."""
        try:
            v1 = self.sr_client.get_version(subject, old_version)
            v2 = self.sr_client.get_version(subject, new_version)
        except SchemaRegistryError as e:
            raise SchemaEvolutionError(f"Failed to fetch schema versions: {e}") from e

        s1 = json.loads(v1.schema.schema_str)
        s2 = json.loads(v2.schema.schema_str)

        return self._compare_schemas(s1, s2)

    def _compare_schemas(self, old_schema: dict, new_schema: dict) -> List[SchemaChange]:
        """Compare two Avro schemas and return list of changes."""
        changes = []
        old_fields = {f['name']: f for f in old_schema.get('fields', [])}
        new_fields = {f['name']: f for f in new_schema.get('fields', [])}

        # Detect removed fields
        for name, field in old_fields.items():
            if name not in new_fields:
                is_breaking = self._is_required_field(field)
                changes.append(SchemaChange(
                    change_type=SchemaChangeType.FIELD_REMOVED,
                    field_name=name,
                    old_value=field,
                    is_breaking=is_breaking
                ))

        # Detect added fields
        for name, field in new_fields.items():
            if name not in old_fields:
                is_breaking = self._is_required_field(field)
                changes.append(SchemaChange(
                    change_type=SchemaChangeType.FIELD_ADDED,
                    field_name=name,
                    new_value=field,
                    is_breaking=is_breaking
                ))

        # Detect modified fields
        for name in old_fields:
            if name in new_fields:
                old_field = old_fields[name]
                new_field = new_fields[name]
                if old_field != new_field:
                    change = self._detect_field_change(name, old_field, new_field)
                    if change:
                        changes.append(change)

        return changes

    def _detect_field_change(self, name: str, old_field: dict, new_field: dict) -> Optional[SchemaChange]:
        """Detect specific type of field modification."""
        if old_field.get('type') != new_field.get('type'):
            is_safe = self._is_safe_type_change(old_field.get('type'), new_field.get('type'))
            return SchemaChange(
                change_type=SchemaChangeType.TYPE_CHANGED,
                field_name=name,
                old_value=old_field.get('type'),
                new_value=new_field.get('type'),
                is_breaking=not is_safe
            )

        if old_field.get('default') != new_field.get('default'):
            return SchemaChange(
                change_type=SchemaChangeType.DEFAULT_CHANGED,
                field_name=name,
                old_value=old_field.get('default'),
                new_value=new_field.get('default'),
                is_breaking=False
            )

        return SchemaChange(
            change_type=SchemaChangeType.FIELD_MODIFIED,
            field_name=name,
            old_value=old_field,
            new_value=new_field,
            is_breaking=False
        )

    @staticmethod
    def _is_required_field(field: dict) -> bool:
        """Check if field is required (no default and not nullable)."""
        field_type = field.get('type')
        if isinstance(field_type, list):
            return 'null' not in field_type
        return field.get('default') is None

    @staticmethod
    def _is_safe_type_change(old_type, new_type) -> bool:
        """Check if type change is safe (widening)."""
        safe_widening = {
            'int': ['long', 'float', 'double'],
            'long': ['float', 'double'],
            'float': ['double'],
        }
        if isinstance(old_type, str) and isinstance(new_type, str):
            return new_type in safe_widening.get(old_type, [])
        return False

    def set_compatibility(self, subject: str, level: CompatibilityLevel) -> bool:
        """Set compatibility level for subject."""
        url = f"{self.base_url}/config/{subject}"
        payload = {"compatibility": level.value}
        try:
            resp = requests.put(url, json=payload)
            resp.raise_for_status()
            logger.info(f"Compatibility set for {subject}: {level.value}")
            return True
        except requests.RequestException as e:
            logger.error(f"Failed to set compatibility: {e}")
            return False


class SchemaEvolutionError(Exception):
    """Raised when schema evolution fails compatibility checks."""
    pass


class DataMeshPipeline:
    """
    Data Mesh pipeline with schema evolution support.

    Implements opt-in/opt-out schema evolution:
    - Opt-in (default): All schema changes propagated automatically
    - Opt-out: Only consumed fields tracked, breaking changes pause pipeline
    """

    def __init__(
        self,
        config: PipelineConfig,
        schema_manager: SchemaEvolutionManager
    ):
        self.config = config
        self.schema_manager = schema_manager
        self.subject = f"{config.source_topic}-value"
        self._state = "RUNNING"
        self._schema_version_history: List[dict] = []

    @property
    def state(self) -> str:
        return self._state

    def handle_schema_change(self, new_schema: dict) -> dict:
        """
        Handle upstream schema change.

        Returns:
            dict with action taken and details
        """
        logger.info(f"[{self.config.pipeline_id}] Processing schema change")

        latest = self.schema_manager.get_latest_schema(self.subject)

        if latest is None:
            # First schema registration
            schema_id = self.schema_manager.register_schema(self.subject, new_schema)
            self._propagate_to_sink(new_schema)
            return {"action": "REGISTERED", "schema_id": schema_id, "is_new": True}

        if latest == new_schema:
            return {"action": "NO_CHANGE", "reason": "Schema identical to latest"}

        # Detect changes
        try:
            # Register will fail if incompatible
            schema_id = self.schema_manager.register_schema(self.subject, new_schema)
        except SchemaEvolutionError as e:
            self._pause_pipeline(f"Incompatible schema: {e}")
            return {"action": "PAUSED", "reason": str(e), "pipeline_id": self.config.pipeline_id}

        # Get detailed changes
        # Note: In production, fetch actual version numbers from registry
        changes = self.schema_manager._compare_schemas(latest, new_schema)

        if self.config.opt_in_schema_evolution:
            result = self._handle_opt_in(new_schema, changes)
        else:
            result = self._handle_opt_out(new_schema, changes)

        self._schema_version_history.append({
            "timestamp": datetime.utcnow().isoformat(),
            "schema_id": schema_id,
            "changes": [c.__dict__ for c in changes],
            "action": result["action"]
        })

        return result

    def _handle_opt_in(self, new_schema: dict, changes: List[SchemaChange]) -> dict:
        """Opt-in: propagate all compatible changes."""
        logger.info(f"[{self.config.pipeline_id}] Opt-in: propagating {len(changes)} changes")
        self._propagate_to_sink(new_schema)
        return {
            "action": "PROPAGATED",
            "pipeline_id": self.config.pipeline_id,
            "changes_count": len(changes),
            "changes": [{"type": c.change_type.value, "field": c.field_name} for c in changes]
        }

    def _handle_opt_out(self, new_schema: dict, changes: List[SchemaChange]) -> dict:
        """Opt-out: check if consumed fields are affected."""
        consumed_set = set(self.config.consumed_fields)
        affected = [c for c in changes if c.field_name in consumed_set and c.is_breaking]

        if affected:
            reason = f"Breaking change affects consumed fields: {[c.field_name for c in affected]}"
            logger.warning(f"[{self.config.pipeline_id}] {reason}")
            self._pause_pipeline(reason)
            self._send_alert("INCOMPATIBLE_SCHEMA_CHANGE", affected)
            return {
                "action": "PAUSED",
                "pipeline_id": self.config.pipeline_id,
                "reason": reason,
                "affected_fields": [c.field_name for c in affected]
            }

        logger.info(f"[{self.config.pipeline_id}] Opt-out: changes do not affect consumed fields")
        return {
            "action": "CONTINUED",
            "pipeline_id": self.config.pipeline_id,
            "reason": "Changes outside consumed fields"
        }

    def _propagate_to_sink(self, schema: dict):
        """Propagate schema changes to Iceberg sink."""
        # In production: call Iceberg REST API or Spark SQL
        logger.info(f"[{self.config.pipeline_id}] Propagating schema to sink: {self.config.sink_table}")
        # Placeholder for actual DDL execution
        ddl = self._generate_iceberg_ddl(schema)
        logger.debug(f"Generated DDL: {ddl}")

    def _generate_iceberg_ddl(self, avro_schema: dict) -> str:
        """Generate Iceberg DDL from Avro schema."""
        type_map = {
            'string': 'STRING',
            'int': 'INT',
            'long': 'BIGINT',
            'float': 'FLOAT',
            'double': 'DOUBLE',
            'boolean': 'BOOLEAN',
            'bytes': 'BINARY',
        }

        fields = []
        for field in avro_schema.get('fields', []):
            name = field['name']
            field_type = field['type']

            if isinstance(field_type, list):
                non_null = [t for t in field_type if t != 'null']
                iceberg_type = type_map.get(non_null[0], 'STRING') if non_null else 'STRING'
                nullable = 'null' in field_type
            else:
                iceberg_type = type_map.get(field_type, 'STRING')
                nullable = False

            null_spec = "" if nullable else " NOT NULL"
            fields.append(f"    {name} {iceberg_type}{null_spec}")

        return f"CREATE TABLE {self.config.sink_table} (\n" + ",\n".join(fields) + "\n)"

    def _pause_pipeline(self, reason: str):
        """Pause pipeline and log reason."""
        self._state = "PAUSED"
        logger.error(f"[{self.config.pipeline_id}] Pipeline PAUSED: {reason}")

    def _send_alert(self, alert_type: str, changes: List[SchemaChange]):
        """Send alert to configured webhook."""
        if not self.config.alert_webhook:
            return

        payload = {
            "pipeline_id": self.config.pipeline_id,
            "domain": self.config.domain,
            "alert_type": alert_type,
            "owner": self.config.owner_email,
            "changes": [{"field": c.field_name, "type": c.change_type.value} for c in changes],
            "timestamp": datetime.utcnow().isoformat()
        }
        try:
            requests.post(self.config.alert_webhook, json=payload, timeout=10)
        except requests.RequestException as e:
            logger.error(f"Failed to send alert: {e}")
