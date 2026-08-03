"""Tests for Hybrid approach."""

import pytest
from unittest.mock import Mock, patch

from datamesh_cdc.schema_evolution import SchemaEvolutionManager, DataMeshPipeline, PipelineConfig, SchemaChangeType
from datamesh_cdc.pipeline_manager import PipelineManager, SelfServeAPI


class TestHybridSchemaEvolution:
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

    def test_validate_silver_ddl_all_fields_present(self, schema_manager):
        schema_manager.sr_client.get_latest_version.return_value = Mock(
            schema=Mock(schema_str='{"type": "record", "name": "Test", "fields": [
                {"name": "id", "type": "long"}, {"name": "name", "type": "string"}
            ]}')
        )
        result = schema_manager.validate_silver_ddl("test", ["id", "name"])
        assert result["valid"] is True
        assert result["missing_fields"] == []

    def test_validate_silver_ddl_missing_field(self, schema_manager):
        schema_manager.sr_client.get_latest_version.return_value = Mock(
            schema=Mock(schema_str='{"type": "record", "name": "Test", "fields": [
                {"name": "id", "type": "long"}
            ]}')
        )
        result = schema_manager.validate_silver_ddl("test", ["id", "missing_field"])
        assert result["valid"] is False
        assert "missing_field" in result["missing_fields"]

    def test_pipeline_never_pauses(self, schema_manager):
        config = PipelineConfig(pipeline_id="test", source_topic="t", sink_table="s", domain="d")
        pipeline = DataMeshPipeline(config, schema_manager)
        result = pipeline.handle_event({"id": 1})
        assert result["action"] == "APPENDED"
        assert pipeline.state == "RUNNING"


class TestHybridPipelineManager:
    @pytest.fixture
    def manager(self):
        with patch('datamesh_cdc.schema_evolution.SchemaRegistryClient'):
            return PipelineManager(state_file=None)

    def test_create_pipeline(self, manager):
        p = manager.create_pipeline("p1", "t1", "s1", "orders")
        assert p.config.pipeline_id == "p1"

    def test_validate_silver_for_pipeline(self, manager):
        manager.create_pipeline("orders-to-bronze", "orders-server.public.orders", "bronze.orders", "orders")
        manager.schema_manager.sr_client.get_latest_version.return_value = Mock(
            schema=Mock(schema_str='{"type": "record", "name": "Order", "fields": [
                {"name": "id", "type": "long"}, {"name": "status", "type": "string"}
            ]}')
        )
        result = manager.validate_silver_for_pipeline("orders-to-bronze", ["id", "status"])
        assert result["valid"] is True

    def test_validate_silver_missing_pipeline(self, manager):
        result = manager.validate_silver_for_pipeline("nonexistent", ["id"])
        assert "error" in result
