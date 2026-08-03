# iceberg_sink.py

## Purpose

Iceberg sink for **strictly typed** CDC data. Tables are created with explicit schemas derived from Avro. Supports schema evolution via `ADD COLUMN` and safe `TYPE WIDENING`.

## Key Classes

### `IcebergCDCSink`

- `create_cdc_table(table_name, avro_schema, namespace="raw")` — creates typed Iceberg table with CDC metadata columns (`_cdc_op`, `_cdc_ts_ms`, `_cdc_source_ts_ms`)
- `apply_schema_evolution(table_name, new_avro_schema, namespace="raw")` — applies changes:
  - **ADD COLUMN** for new fields
  - **TYPE WIDENING** for safe conversions (`int→long`, `float→double`)
- `create_scd2_view(table_name, namespace="raw")` — generates SQL for SCD Type 2 view (latest state per key)
- `get_table_metadata(table_name, namespace="raw")` — returns table info (location, schema, snapshots)

## Avro to Iceberg Type Mapping

| Avro | Iceberg |
|------|---------|
| `string` | `STRING` |
| `int` | `INT` |
| `long` | `BIGINT` |
| `float` | `FLOAT` |
| `double` | `DOUBLE` |
| `boolean` | `BOOLEAN` |
| `bytes` | `BINARY` |

Union types `["null", "string"]` are mapped to **nullable** columns.

## Usage Example

```python
from datamesh_cdc.iceberg_sink import IcebergCDCSink

sink = IcebergCDCSink()
sink.create_cdc_table("orders", avro_schema)
changes = sink.apply_schema_evolution("orders", new_avro_schema)
# {"added": ["promo_code"], "widened": [], "errors": []}
```
