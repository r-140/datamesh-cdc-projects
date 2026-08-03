"""
Tests for Data Mesh CDC Platform.

Uses pytest and testcontainers for integration testing.
"""

import json
import pytest
from unittest.mock import Mock, patch

from datamesh_cdc.schema_evolution import (
    SchemaEvolutionManager,
    DataMeshPipeline,
    PipelineConfig,
    SchemaChangeType,
    CompatibilityLevel,
    SchemaEvolutionError
)
from datamesh_cdc.pipeline_manager import PipelineManager, SelfServeAPI


class TestSchemaCompatibility:
    """Test schema compatibility checks."""

    @pytest.fixture
    def schema_manager(self):
        with patch('datamesh_cdc.schema_evolution.SchemaRegistryClient'):
            manager = SchemaEvolutionManager("http://localhost:8081")
            manager.sr_client = Mock()
            return manager

    def test_add_optional_field_is_compatible(self, schema_manager):
        """Adding optional field should be backward compatible."""
        old = {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"}
            ]
        }
        new = {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": ["null", "string"], "default": None}
            ]
        }

        changes = schema_manager._compare_schemas(old, new)
        assert len(changes) == 1
        assert changes[0].change_type == SchemaChangeType.FIELD_ADDED
        assert changes[0].field_name == "email"
        assert not changes[0].is_breaking

    def test_add_required_field_is_breaking(self, schema_manager):
        """Adding required field without default is breaking."""
        old = {
            "type": "record", "name": "Test",
            "fields": [{"name": "id", "type": "long"}]
        }
        new = {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "required_field", "type": "string"}
            ]
        }

        changes = schema_manager._compare_schemas(old, new)
        assert changes[0].is_breaking

    def test_remove_required_field_is_breaking(self, schema_manager):
        """Removing required field is breaking."""
        old = {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"}
            ]
        }
        new = {
            "type": "record", "name": "Test",
            "fields": [{"name": "id", "type": "long"}]
        }

        changes = schema_manager._compare_schemas(old, new)
        assert changes[0].change_type == SchemaChangeType.FIELD_REMOVED
        assert changes[0].is_breaking

    def test_remove_optional_field_is_safe(self, schema_manager):
        """Removing optional field is safe."""
        old = {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "notes", "type": ["null", "string"], "default": None}
            ]
        }
        new = {
            "type": "record", "name": "Test",
            "fields": [{"name": "id", "type": "long"}]
        }

        changes = schema_manager._compare_schemas(old, new)
        assert not changes[0].is_breaking

    def test_type_widening_int_to_long(self, schema_manager):
        """Widening int to long should be safe."""
        assert schema_manager._is_safe_type_change("int", "long")

    def test_type_narrowing_long_to_int(self, schema_manager):
        """Narrowing long to int should be unsafe."""
        assert not schema_manager._is_safe_type_change("long", "int")


class TestDataMeshPipeline:
    """Test pipeline opt-in/opt-out behavior."""

    @pytest.fixture
    def pipeline_manager(self):
        with patch('datamesh_cdc.schema_evolution.SchemaRegistryClient'):
            sr_manager = SchemaEvolutionManager("http://localhost:8081")
            sr_manager.sr_client = Mock()
            return sr_manager

    def test_opt_in_propagates_all_changes(self, pipeline_manager):
        """Opt-in pipeline should propagate all compatible changes."""
        config = PipelineConfig(
            pipeline_id="test-opt-in",
            source_topic="test.topic",
            sink_table="raw.test",
            domain="test",
            opt_in_schema_evolution=True
        )
        pipeline = DataMeshPipeline(config, pipeline_manager)

        new_schema = {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "new_field", "type": ["null", "string"], "default": None}
            ]
        }

        with patch.object(pipeline, '_propagate_to_sink'):
            result = pipeline.handle_schema_change(new_schema)
            assert result["action"] == "PROPAGATED"

    def test_opt_out_pauses_on_consumed_field_removal(self, pipeline_manager):
        """Opt-out pipeline should pause when consumed field is removed."""
        config = PipelineConfig(
            pipeline_id="test-opt-out",
            source_topic="test.topic",
            sink_table="raw.test",
            domain="test",
            opt_in_schema_evolution=False,
            consumed_fields=["amount"]
        )
        pipeline = DataMeshPipeline(config, pipeline_manager)

        # Simulate existing schema
        pipeline_manager.sr_client.get_latest_version.return_value = Mock(
            version=1,
            schema=Mock(schema_str=json.dumps({
                "type": "record", "name": "Test",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "amount", "type": "double"}
                ]
            }))
        )

        new_schema = {
            "type": "record", "name": "Test",
            "fields": [{"name": "id", "type": "long"}]
            # amount removed!
        }

        result = pipeline.handle_schema_change(new_schema)
        assert result["action"] == "PAUSED"
        assert "amount" in result["affected_fields"]

    def test_opt_out_continues_on_unrelated_change(self, pipeline_manager):
        """Opt-out pipeline should continue if consumed fields unaffected."""
        config = PipelineConfig(
            pipeline_id="test-opt-out-safe",
            source_topic="test.topic",
            sink_table="raw.test",
            domain="test",
            opt_in_schema_evolution=False,
            consumed_fields=["id", "amount"]
        )
        pipeline = DataMeshPipeline(config, pipeline_manager)

        pipeline_manager.sr_client.get_latest_version.return_value = Mock(
            version=1,
            schema=Mock(schema_str=json.dumps({
                "type": "record", "name": "Test",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "amount", "type": "double"},
                    {"name": "notes", "type": ["null", "string"], "default": None}
                ]
            }))
        )

        new_schema = {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "amount", "type": "double"},
                {"name": "notes", "type": ["null", "string"], "default": None},
                {"name": "new_meta", "type": ["null", "string"], "default": None}
            ]
        }

        result = pipeline.handle_schema_change(new_schema)
        assert result["action"] == "CONTINUED"


class TestPipelineManager:
    """Test pipeline manager operations."""

    @pytest.fixture
    def manager(self):
        with patch('datamesh_cdc.schema_evolution.SchemaRegistryClient'):
            return PipelineManager(
                schema_registry_url="http://localhost:8081",
                state_file=None
            )

    def test_create_pipeline(self, manager):
        """Pipeline creation should succeed."""
        pipeline = manager.create_pipeline(
            pipeline_id="test-pipe",
            source_topic="domain.orders",
            sink_table="raw.orders",
            domain="orders"
        )
        assert pipeline.config.pipeline_id == "test-pipe"
        assert pipeline.config.domain == "orders"

    def test_duplicate_pipeline_raises_error(self, manager):
        """Duplicate pipeline ID should raise error."""
        manager.create_pipeline(
            pipeline_id="dup",
            source_topic="t1",
            sink_table="s1",
            domain="d1"
        )
        with pytest.raises(ValueError):
            manager.create_pipeline(
                pipeline_id="dup",
                source_topic="t2",
                sink_table="s2",
                domain="d2"
            )

    def test_list_pipelines_by_domain(self, manager):
        """Listing should filter by domain."""
        manager.create_pipeline("p1", "t1", "s1", "orders")
        manager.create_pipeline("p2", "t2", "s2", "customers")
        manager.create_pipeline("p3", "t3", "s3", "orders")

        orders_pipes = manager.list_pipelines(domain="orders")
        assert len(orders_pipes) == 2

    def test_domain_stats(self, manager):
        """Domain stats should aggregate correctly."""
        manager.create_pipeline("p1", "t1", "s1", "orders", opt_in_schema_evolution=True)
        manager.create_pipeline("p2", "t2", "s2", "orders", opt_in_schema_evolution=False)

        stats = manager.get_domain_stats("orders")
        assert stats["total_pipelines"] == 2
        assert stats["opt_in_count"] == 1
        assert stats["opt_out_count"] == 1


class TestSelfServeAPI:
    """Test self-serve API for domain teams."""

    @pytest.fixture
    def api(self):
        with patch('datamesh_cdc.schema_evolution.SchemaRegistryClient'):
            manager = PipelineManager(state_file=None)
            return SelfServeAPI(manager)

    def test_create_pipeline_request(self, api):
        """Valid request should create pipeline."""
        request = {
            "pipeline_id": "api-test",
            "source_topic": "domain.test",
            "sink_table": "raw.test",
            "domain": "test-domain",
            "opt_in_schema_evolution": False,
            "consumed_fields": ["id", "name"]
        }
        result = api.create_pipeline_request(request)
        assert result["status"] == "created"
        assert result["opt_in"] == False

    def test_missing_fields_returns_error(self, api):
        """Missing required fields should return error."""
        request = {"pipeline_id": "bad"}
        result = api.create_pipeline_request(request)
        assert "error" in result
        assert "Missing required fields" in result["error"]
