"""Tests for Schema-on-Write (Strict) approach."""

import json
import pytest
from unittest.mock import Mock, patch

from datamesh_cdc.schema_evolution import (
    SchemaEvolutionManager, DataMeshPipeline, PipelineConfig,
    SchemaChangeType, CompatibilityLevel, SchemaEvolutionError
)
from datamesh_cdc.pipeline_manager import PipelineManager, SelfServeAPI


class TestSchemaCompatibility:
    @pytest.fixture
    def schema_manager(self):
        with patch('datamesh_cdc.schema_evolution.SchemaRegistryClient'):
            manager = SchemaEvolutionManager("http://localhost:8081")
            manager.sr_client = Mock()
            return manager

    def test_add_optional_field_is_compatible(self, schema_manager):
        old = {"type": "record", "name": "Test", "fields": [
            {"name": "id", "type": "long"}, {"name": "name", "type": "string"}
        ]}
        new = {"type": "record", "name": "Test", "fields": [
            {"name": "id", "type": "long"}, {"name": "name", "type": "string"},
            {"name": "email", "type": ["null", "string"], "default": None}
        ]}
        changes = schema_manager._compare_schemas(old, new)
        assert len(changes) == 1
        assert changes[0].change_type == SchemaChangeType.FIELD_ADDED
        assert changes[0].field_name == "email"
        assert not changes[0].is_breaking

    def test_add_required_field_is_breaking(self, schema_manager):
        old = {"type": "record", "name": "Test", "fields": [{"name": "id", "type": "long"}]}
        new = {"type": "record", "name": "Test", "fields": [
            {"name": "id", "type": "long"}, {"name": "required_field", "type": "string"}
        ]}
        changes = schema_manager._compare_schemas(old, new)
        assert changes[0].is_breaking

    def test_remove_required_field_is_breaking(self, schema_manager):
        old = {"type": "record", "name": "Test", "fields": [
            {"name": "id", "type": "long"}, {"name": "name", "type": "string"}
        ]}
        new = {"type": "record", "name": "Test", "fields": [{"name": "id", "type": "long"}]}
        changes = schema_manager._compare_schemas(old, new)
        assert changes[0].change_type == SchemaChangeType.FIELD_REMOVED
        assert changes[0].is_breaking

    def test_remove_optional_field_is_safe(self, schema_manager):
        old = {"type": "record", "name": "Test", "fields": [
            {"name": "id", "type": "long"}, {"name": "notes", "type": ["null", "string"], "default": None}
        ]}
        new = {"type": "record", "name": "Test", "fields": [{"name": "id", "type": "long"}]}
        changes = schema_manager._compare_schemas(old, new)
        assert not changes[0].is_breaking

    def test_type_widening_int_to_long(self, schema_manager):
        assert schema_manager._is_safe_type_change("int", "long")

    def test_type_narrowing_long_to_int(self, schema_manager):
        assert not schema_manager._is_safe_type_change("long", "int")


class TestDataMeshPipeline:
    @pytest.fixture
    def pipeline_manager(self):
        with patch('datamesh_cdc.schema_evolution.SchemaRegistryClient'):
            sr_manager = SchemaEvolutionManager("http://localhost:8081")
            sr_manager.sr_client = Mock()
            return sr_manager

    def test_opt_in_propagates_all_changes(self, pipeline_manager):
        config = PipelineConfig(pipeline_id="test-opt-in", source_topic="test.topic",
                                sink_table="raw.test", domain="test", opt_in_schema_evolution=True)
        pipeline = DataMeshPipeline(config, pipeline_manager)
        new_schema = {"type": "record", "name": "Test", "fields": [
            {"name": "id", "type": "long"}, {"name": "new_field", "type": ["null", "string"], "default": None}
        ]}
        with patch.object(pipeline, '_propagate_to_sink'):
            result = pipeline.handle_schema_change(new_schema)
            assert result["action"] == "PROPAGATED"

    def test_opt_out_pauses_on_consumed_field_removal(self, pipeline_manager):
        config = PipelineConfig(pipeline_id="test-opt-out", source_topic="test.topic",
                                sink_table="raw.test", domain="test", opt_in_schema_evolution=False,
                                consumed_fields=["amount"])
        pipeline = DataMeshPipeline(config, pipeline_manager)
        pipeline_manager.sr_client.get_latest_version.return_value = Mock(
            version=1, schema=Mock(schema_str=json.dumps({
                "type": "record", "name": "Test",
                "fields": [{"name": "id", "type": "long"}, {"name": "amount", "type": "double"}]
            }))
        )
        new_schema = {"type": "record", "name": "Test", "fields": [{"name": "id", "type": "long"}]}
        result = pipeline.handle_schema_change(new_schema)
        assert result["action"] == "PAUSED"
        assert "amount" in result["affected_fields"]

    def test_opt_out_continues_on_unrelated_change(self, pipeline_manager):
        config = PipelineConfig(pipeline_id="test-opt-out-safe", source_topic="test.topic",
                                sink_table="raw.test", domain="test", opt_in_schema_evolution=False,
                                consumed_fields=["id", "amount"])
        pipeline = DataMeshPipeline(config, pipeline_manager)
        pipeline_manager.sr_client.get_latest_version.return_value = Mock(
            version=1, schema=Mock(schema_str=json.dumps({
                "type": "record", "name": "Test", "fields": [
                    {"name": "id", "type": "long"}, {"name": "amount", "type": "double"},
                    {"name": "notes", "type": ["null", "string"], "default": None}
                ]
            }))
        )
        new_schema = {"type": "record", "name": "Test", "fields": [
            {"name": "id", "type": "long"}, {"name": "amount", "type": "double"},
            {"name": "notes", "type": ["null", "string"], "default": None},
            {"name": "new_meta", "type": ["null", "string"], "default": None}
        ]}
        result = pipeline.handle_schema_change(new_schema)
        assert result["action"] == "CONTINUED"


class TestPipelineManager:
    @pytest.fixture
    def manager(self):
        with patch('datamesh_cdc.schema_evolution.SchemaRegistryClient'):
            return PipelineManager(state_file=None)

    def test_create_pipeline(self, manager):
        pipeline = manager.create_pipeline("test-pipe", "domain.orders", "raw.orders", "orders")
        assert pipeline.config.pipeline_id == "test-pipe"

    def test_duplicate_pipeline_raises_error(self, manager):
        manager.create_pipeline("dup", "t1", "s1", "d1")
        with pytest.raises(ValueError):
            manager.create_pipeline("dup", "t2", "s2", "d2")

    def test_list_pipelines_by_domain(self, manager):
        manager.create_pipeline("p1", "t1", "s1", "orders")
        manager.create_pipeline("p2", "t2", "s2", "customers")
        assert len(manager.list_pipelines(domain="orders")) == 1

    def test_domain_stats(self, manager):
        manager.create_pipeline("p1", "t1", "s1", "orders", opt_in_schema_evolution=True)
        manager.create_pipeline("p2", "t2", "s2", "orders", opt_in_schema_evolution=False)
        stats = manager.get_domain_stats("orders")
        assert stats["opt_in_count"] == 1
        assert stats["opt_out_count"] == 1
