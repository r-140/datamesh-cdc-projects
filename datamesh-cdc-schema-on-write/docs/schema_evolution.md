# schema_evolution.py

## Purpose

Core module for **strict schema evolution**. Registers schemas in Confluent Schema Registry, detects changes, and decides whether to propagate or pause the pipeline.

## Key Classes

### `SchemaEvolutionManager`

- `register_schema(subject, schema)` — registers schema; raises `SchemaEvolutionError` if incompatible
- `get_latest_schema(subject)` — fetches latest schema from registry
- `get_schema_changes(subject, old_v, new_v)` — returns list of `SchemaChange` objects
- `_compare_schemas(old, new)` — detects added/removed/modified fields
- `_is_safe_type_change(old_type, new_type)` — allows widening: `int→long→float→double`

### `DataMeshPipeline`

- `handle_schema_change(new_schema)` — main entry point
  - **Opt-in**: propagates all compatible changes automatically
  - **Opt-out**: checks `consumed_fields`; pauses if breaking change affects them
- `_propagate_to_sink(schema)` — generates Iceberg DDL
- `_pause_pipeline(reason)` — sets state to `PAUSED`, logs error
- `_send_alert(type, changes)` — sends webhook notification to domain owner

### `PipelineConfig`

Dataclass holding pipeline configuration:
- `opt_in_schema_evolution` — `True` = accept all changes, `False` = protect consumed fields
- `consumed_fields` — fields the downstream pipeline depends on (opt-out mode)
- `compatibility_level` — `BACKWARD`, `FORWARD`, `FULL`, or `NONE`

## Schema Change Types

| Type | Breaking? | Example |
|------|-----------|---------|
| `FIELD_ADDED` | Only if required without default | `promo_code` with default → safe |
| `FIELD_REMOVED` | Only if field was required | Dropping `total_amount` → breaking |
| `TYPE_CHANGED` | Only if not safe widening | `int→long` → safe; `long→int` → breaking |
| `DEFAULT_CHANGED` | Never | Changing default value |

## Usage Example

```python
from datamesh_cdc.schema_evolution import SchemaEvolutionManager, DataMeshPipeline, PipelineConfig

manager = SchemaEvolutionManager("http://localhost:8081")
config = PipelineConfig(
    pipeline_id="orders-to-reporting",
    source_topic="orders-server.public.orders",
    sink_table="reporting.orders_summary",
    domain="orders",
    opt_in_schema_evolution=False,
    consumed_fields=["id", "total_amount"]
)
pipeline = DataMeshPipeline(config, manager)

result = pipeline.handle_schema_change(new_schema)
# {"action": "PAUSED", "affected_fields": ["total_amount"], ...}
```
