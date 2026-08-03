"""
Data Mesh Pipeline Manager.

Manages multiple CDC pipelines across domains with:
- Pipeline lifecycle (create, pause, resume, delete)
- Schema evolution handling
- Domain ownership tracking
- Self-serve API
"""

import json
import logging
from dataclasses import asdict
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path

from .schema_evolution import (
    SchemaEvolutionManager,
    DataMeshPipeline,
    PipelineConfig,
    CompatibilityLevel
)
from .iceberg_sink import IcebergCDCSink

logger = logging.getLogger(__name__)


class PipelineManager:
    """Central manager for all Data Mesh pipelines."""

    def __init__(
        self,
        schema_registry_url: str = "http://localhost:8081",
        iceberg_catalog_uri: str = "http://localhost:8181",
        state_file: Optional[str] = None
    ):
        self.schema_manager = SchemaEvolutionManager(schema_registry_url)
        self.iceberg = IcebergCDCSink(catalog_uri=iceberg_catalog_uri)
        self.pipelines: Dict[str, DataMeshPipeline] = {}
        self.state_file = Path(state_file) if state_file else None

        if self.state_file and self.state_file.exists():
            self._load_state()

    def create_pipeline(
        self,
        pipeline_id: str,
        source_topic: str,
        sink_table: str,
        domain: str,
        opt_in_schema_evolution: bool = True,
        consumed_fields: Optional[List[str]] = None,
        owner_email: str = "",
        alert_webhook: Optional[str] = None
    ) -> DataMeshPipeline:
        """
        Create new CDC pipeline.

        Args:
            pipeline_id: Unique pipeline identifier
            source_topic: Kafka source topic (e.g., orders-server.public.orders)
            sink_table: Target Iceberg table name
            domain: Domain ownership (e.g., "orders", "customers")
            opt_in_schema_evolution: If True, all schema changes propagated
            consumed_fields: Fields to track (for opt-out mode)
            owner_email: Domain owner contact
            alert_webhook: Webhook for incompatible change alerts

        Returns:
            Created pipeline instance
        """
        if pipeline_id in self.pipelines:
            raise ValueError(f"Pipeline {pipeline_id} already exists")

        config = PipelineConfig(
            pipeline_id=pipeline_id,
            source_topic=source_topic,
            sink_table=sink_table,
            domain=domain,
            opt_in_schema_evolution=opt_in_schema_evolution,
            consumed_fields=consumed_fields or [],
            owner_email=owner_email,
            alert_webhook=alert_webhook
        )

        pipeline = DataMeshPipeline(config, self.schema_manager)
        self.pipelines[pipeline_id] = pipeline

        # Create sink table if it doesn't exist
        # In production: fetch schema from source and create
        logger.info(f"Pipeline created: {pipeline_id} (domain: {domain})")
        self._save_state()

        return pipeline

    def get_pipeline(self, pipeline_id: str) -> Optional[DataMeshPipeline]:
        """Get pipeline by ID."""
        return self.pipelines.get(pipeline_id)

    def list_pipelines(self, domain: Optional[str] = None) -> List[Dict]:
        """List all pipelines, optionally filtered by domain."""
        result = []
        for pid, pipe in self.pipelines.items():
            if domain and pipe.config.domain != domain:
                continue
            result.append({
                "pipeline_id": pid,
                "domain": pipe.config.domain,
                "source_topic": pipe.config.source_topic,
                "sink_table": pipe.config.sink_table,
                "state": pipe.state,
                "opt_in": pipe.config.opt_in_schema_evolution,
                "owner": pipe.config.owner_email
            })
        return result

    def handle_schema_change(self, pipeline_id: str, new_schema: dict) -> dict:
        """Route schema change to specific pipeline."""
        pipeline = self.pipelines.get(pipeline_id)
        if not pipeline:
            return {"error": f"Pipeline {pipeline_id} not found"}

        result = pipeline.handle_schema_change(new_schema)
        self._save_state()
        return result

    def get_domain_stats(self, domain: str) -> Dict:
        """Get statistics for a domain."""
        domain_pipes = [p for p in self.pipelines.values() if p.config.domain == domain]

        return {
            "domain": domain,
            "total_pipelines": len(domain_pipes),
            "running": sum(1 for p in domain_pipes if p.state == "RUNNING"),
            "paused": sum(1 for p in domain_pipes if p.state == "PAUSED"),
            "opt_in_count": sum(1 for p in domain_pipes if p.config.opt_in_schema_evolution),
            "opt_out_count": sum(1 for p in domain_pipes if not p.config.opt_in_schema_evolution),
        }

    def _save_state(self):
        """Persist pipeline state to file."""
        if not self.state_file:
            return

        state = {
            "version": "1.0",
            "updated_at": datetime.utcnow().isoformat(),
            "pipelines": {}
        }

        for pid, pipe in self.pipelines.items():
            state["pipelines"][pid] = {
                "config": asdict(pipe.config),
                "state": pipe.state,
                "history": pipe._schema_version_history
            }

        self.state_file.write_text(json.dumps(state, indent=2))

    def _load_state(self):
        """Load pipeline state from file."""
        try:
            data = json.loads(self.state_file.read_text())
            for pid, pdata in data.get("pipelines", {}).items():
                config = PipelineConfig(**pdata["config"])
                pipe = DataMeshPipeline(config, self.schema_manager)
                pipe._state = pdata["state"]
                pipe._schema_version_history = pdata.get("history", [])
                self.pipelines[pid] = pipe
            logger.info(f"Loaded {len(self.pipelines)} pipelines from state")
        except Exception as e:
            logger.error(f"Failed to load state: {e}")


class SelfServeAPI:
    """
    Self-serve API for domain teams to manage their pipelines.

    In production: FastAPI/Flask REST endpoints
    """

    def __init__(self, pipeline_manager: PipelineManager):
        self.manager = pipeline_manager

    def create_pipeline_request(self, request: dict) -> dict:
        """Handle pipeline creation request from domain team."""
        required = ["pipeline_id", "source_topic", "sink_table", "domain"]
        missing = [f for f in required if f not in request]
        if missing:
            return {"error": f"Missing required fields: {missing}"}

        try:
            pipeline = self.manager.create_pipeline(
                pipeline_id=request["pipeline_id"],
                source_topic=request["source_topic"],
                sink_table=request["sink_table"],
                domain=request["domain"],
                opt_in_schema_evolution=request.get("opt_in_schema_evolution", True),
                consumed_fields=request.get("consumed_fields"),
                owner_email=request.get("owner_email", ""),
                alert_webhook=request.get("alert_webhook")
            )
            return {
                "status": "created",
                "pipeline_id": pipeline.config.pipeline_id,
                "opt_in": pipeline.config.opt_in_schema_evolution
            }
        except ValueError as e:
            return {"error": str(e)}

    def list_domain_pipelines(self, domain: str) -> List[Dict]:
        """List pipelines for a specific domain (self-serve)."""
        return self.manager.list_pipelines(domain=domain)
