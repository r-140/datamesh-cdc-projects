# Schema Evolution

## Protection Levels

### 1. Schema Registry (hard protection)
- Compatibility mode: **BACKWARD** (default)
- New schema must be readable by old consumers
- Breaking changes are rejected with **HTTP 409**

### 2. Debezium ExtractNewRecordState (soft protection)
- `transforms.unwrap.delete.handling.mode=rewrite`
- `transforms.unwrap.drop.tombstones=false`
- On DROP COLUMN: fills `null` instead of crashing

## BACKWARD Compatibility Rules

| Operation | Result | Rule |
|-----------|--------|------|
| ADD COLUMN optional | Accepted | New field with `default=null` |
| ADD COLUMN required | Rejected | No default — old readers break |
| DROP COLUMN | Debezium handles | Schema Registry 409, but Debezium null-fills |
| RENAME COLUMN | Rejected | This is delete + add |
| CHANGE TYPE | Rejected | TYPE_MISMATCH |

## API Check

```bash
# Register new schema
curl -X POST http://localhost:8081/subjects/orders-value/versions   -H "Content-Type: application/vnd.schemaregistry.v1+json"   -d '{"schema": "{\"type\":\"record\"...}"}'

# Check compatibility
curl -X POST http://localhost:8081/compatibility/subjects/orders-value/versions/latest   -d '{"schema": "..."}'
```

## Switching Compatibility Mode

```bash
# FULL — stricter than BACKWARD
curl -X PUT http://localhost:8081/config/orders-value   -d '{"compatibility": "FULL"}'
```
