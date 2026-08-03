"""
Pipeline Manager — Schema-on-Read.

Manages CDC pipelines where Bronze stores JSON and schema is applied at Silver/Gold.
"""

import json
import logging
from dataclasses import asdict
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path

from .schema_evolution import SchemaEvolutionManager, DataMeshPipeline, PipelineConfig

logger = logging.getLogger(__name__)


class PipelineManager:
    def __init__(self, schema_registry_url: str = "http://localhost:8081",
                 state_file: Optional[str] = None):
        self.schema_manager = SchemaEvolutionManager(schema_registry_url)
        self.pipelines: Dict[str, DataMeshPipeline] = {}
        self.state_file = Path(state_file) if state_file else None
        if self.state_file and self.state_file.exists():
            self._load_state()

    def create_pipeline(self, pipeline_id: str, source_topic: str, sink_table: str,
                        domain: str, owner_email: str = "",
                        alert_webhook: Optional[str] = None,
                        track_schema_history: bool = True) -> DataMeshPipeline:
        if pipeline_id in self.pipelines:
            raise ValueError(f"Pipeline {pipeline_id} already exists")
        config = PipelineConfig(
            pipeline_id=pipeline_id, source_topic=source_topic,
            sink_table=sink_table, domain=domain, owner_email=owner_email,
            alert_webhook=alert_webhook, track_schema_history=track_schema_history
        )
        pipeline = DataMeshPipeline(config, self.schema_manager)
        self.pipelines[pipeline_id] = pipeline
        logger.info(f"Pipeline created: {pipeline_id} (domain: {domain})")
        self._save_state()
        return pipeline

    def get_pipeline(self, pipeline_id: str) -> Optional[DataMeshPipeline]:
        return self.pipelines.get(pipeline_id)

    def list_pipelines(self, domain: Optional[str] = None) -> List[Dict]:
        result = []
        for pid, pipe in self.pipelines.items():
            if domain and pipe.config.domain != domain:
                continue
            result.append({
                "pipeline_id": pid, "domain": pipe.config.domain,
                "source_topic": pipe.config.source_topic,
                "sink_table": pipe.config.sink_table,
                "state": pipe.state,
                "owner": pipe.config.owner_email
            })
        return result

    def get_domain_stats(self, domain: str) -> Dict:
        domain_pipes = [p for p in self.pipelines.values() if p.config.domain == domain]
        return {
            "domain": domain, "total_pipelines": len(domain_pipes),
            "running": sum(1 for p in domain_pipes if p.state == "RUNNING"),
            "schema_changes_logged": sum(len(p._schema_version_history) for p in domain_pipes),
        }

    def _save_state(self):
        if not self.state_file:
            return
        state = {"version": "1.0", "updated_at": datetime.utcnow().isoformat(), "pipelines": {}}
        for pid, pipe in self.pipelines.items():
            state["pipelines"][pid] = {
                "config": asdict(pipe.config), "state": pipe.state,
                "history": pipe._schema_version_history
            }
        self.state_file.write_text(json.dumps(state, indent=2))

    def _load_state(self):
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
    def __init__(self, pipeline_manager: PipelineManager):
        self.manager = pipeline_manager

    def create_pipeline_request(self, request: dict) -> dict:
        required = ["pipeline_id", "source_topic", "sink_table", "domain"]
        missing = [f for f in required if f not in request]
        if missing:
            return {"error": f"Missing required fields: {missing}"}
        try:
            pipeline = self.manager.create_pipeline(
                pipeline_id=request["pipeline_id"], source_topic=request["source_topic"],
                sink_table=request["sink_table"], domain=request["domain"],
                owner_email=request.get("owner_email", ""),
                alert_webhook=request.get("alert_webhook"),
                track_schema_history=request.get("track_schema_history", True)
            )
            return {"status": "created", "pipeline_id": pipeline.config.pipeline_id}
        except ValueError as e:
            return {"error": str(e)}

    def list_domain_pipelines(self, domain: str) -> List[Dict]:
        return self.manager.list_pipelines(domain=domain)
