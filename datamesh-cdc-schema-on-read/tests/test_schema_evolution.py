"""Tests for Schema-on-Read (Flexible) approach."""

import pytest
from unittest.mock import Mock, patch

from datamesh_cdc.schema_evolution import SchemaEvolutionManager, DataMeshPipeline, PipelineConfig, SchemaChangeType
from datamesh_cdc.pipeline_manager import PipelineManager, SelfServeAPI


class TestSchemaOnRead:
    @pytest.fixture
    def schema_manager(self):
        with patch('datamesh_cdc.schema_evolution.SchemaRegistryClient'):
            manager = SchemaEvolutionManager("http://localhost:8081")
            manager.sr_client = Mock()
            return manager

    def test_detect_add_field(self, schema_manager):
        old = {"type": "record", "name": "Test", "fields": [{"name": "id", "type": "long"}]}
        new = {"type": "record", "name": "Test", "fields": [
            {"name": "id", "type": "long"}, {"name": "email", "type": "string"}
        ]}
        changes = schema_manager.detect_schema_changes(old, new)
        assert len(changes) == 1
        assert changes[0].change_type == SchemaChangeType.FIELD_ADDED

    def test_detect_remove_field(self, schema_manager):
        old = {"type": "record", "name": "Test", "fields": [
            {"name": "id", "type": "long"}, {"name": "name", "type": "string"}
        ]}
        new = {"type": "record", "name": "Test", "fields": [{"name": "id", "type": "long"}]}
        changes = schema_manager.detect_schema_changes(old, new)
        assert changes[0].change_type == SchemaChangeType.FIELD_REMOVED

    def test_pipeline_never_pauses(self, schema_manager):
        config = PipelineConfig(pipeline_id="test", source_topic="t", sink_table="s", domain="d")
        pipeline = DataMeshPipeline(config, schema_manager)
        result = pipeline.handle_event({"id": 1})
        assert result["action"] == "APPENDED"
        assert pipeline.state == "RUNNING"

    def test_pipeline_logs_schema_changes(self, schema_manager):
        config = PipelineConfig(pipeline_id="test", source_topic="t", sink_table="s", domain="d")
        pipeline = DataMeshPipeline(config, schema_manager)
        schema_manager.sr_client.get_latest_version.return_value = Mock(
            schema=Mock(schema_str='{"type": "record", "name": "Test", "fields": [{"name": "id", "type": "long"}]}')
        )
        new_schema = {"type": "record", "name": "Test", "fields": [
            {"name": "id", "type": "long"}, {"name": "new", "type": "string"}
        ]}
        result = pipeline.handle_event({"id": 1, "new": "x"}, current_schema=new_schema)
        assert result["action"] == "APPENDED"
        assert result.get("schema_changes_detected") == 1


class TestPipelineManager:
    @pytest.fixture
    def manager(self):
        with patch('datamesh_cdc.schema_evolution.SchemaRegistryClient'):
            return PipelineManager(state_file=None)

    def test_create_pipeline(self, manager):
        p = manager.create_pipeline("p1", "t1", "s1", "orders")
        assert p.config.pipeline_id == "p1"

    def test_all_pipelines_running(self, manager):
        manager.create_pipeline("p1", "t1", "s1", "orders")
        manager.create_pipeline("p2", "t2", "s2", "customers")
        stats = manager.get_domain_stats("orders")
        assert stats["running"] == 1
        assert stats["schema_changes_logged"] == 0
