# pipeline_manager.py

## Purpose

Manages the lifecycle of CDC pipelines across Data Mesh domains. Provides a **self-serve API** for domain teams to create and monitor their own pipelines.

## Key Classes

### `PipelineManager`

Central registry of all pipelines.

- `create_pipeline(...)` — creates a new pipeline with config
- `get_pipeline(pipeline_id)` — retrieves pipeline by ID
- `list_pipelines(domain=None)` — lists all or domain-filtered pipelines
- `handle_schema_change(pipeline_id, new_schema)` — routes schema change to specific pipeline
- `get_domain_stats(domain)` — returns aggregate stats (running/paused, opt-in/opt-out counts)
- `_save_state()` / `_load_state()` — persists pipeline state to JSON file

### `SelfServeAPI`

API layer for domain teams.

- `create_pipeline_request(request)` — validates required fields, creates pipeline
- `list_domain_pipelines(domain)` — returns pipelines owned by a domain

## State Persistence

Pipeline state is saved to a JSON file (default: `/tmp/datamesh_state.json`):

```json
{
  "version": "1.0",
  "updated_at": "2024-01-15T10:00:00",
  "pipelines": {
    "orders-to-analytics": {
      "config": { ... },
      "state": "RUNNING",
      "history": [ ... ]
    }
  }
}
```

## Usage Example

```python
from datamesh_cdc.pipeline_manager import PipelineManager, SelfServeAPI

manager = PipelineManager(state_file="/tmp/state.json")
api = SelfServeAPI(manager)

# Domain team creates their pipeline
result = api.create_pipeline_request({
    "pipeline_id": "my-pipeline",
    "source_topic": "domain.events",
    "sink_table": "raw.events",
    "domain": "my-domain",
    "opt_in_schema_evolution": True
})
```
