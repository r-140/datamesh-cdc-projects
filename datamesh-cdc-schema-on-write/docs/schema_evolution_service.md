# Schema Evolution Service

## Purpose

REST service that:
1. Tracks schema changes in Schema Registry
2. Evaluates impact on downstream pipelines
3. Makes decision: PROPAGATE / PAUSE / CONTINUE

## API Endpoints

### POST /api/v1/schema/validate
Validate new schema before registration.

```json
{
  "subject": "orders-value",
  "schema": {
    "type": "record",
    "name": "Order",
    "fields": [
      {"name": "id", "type": "long"},
      {"name": "promo_code", "type": ["null", "string"], "default": null}
    ]
  }
}
```

### GET /api/v1/pipelines/{domain}
List pipelines and their status.

```json
{
  "domain": "orders",
  "pipelines": [
    {
      "name": "orders-to-analytics",
      "mode": "opt-in",
      "status": "RUNNING",
      "consumed_fields": ["id", "status", "total_amount"]
    }
  ]
}
```

### POST /api/v1/pipelines/{pipeline}/mode
Change subscription mode.

```json
{
  "mode": "opt-out"
}
```

## Logic

```
1. Receive new schema from producer
2. Request current schema from Schema Registry
3. Compare diff (fields added/removed/type-changed)
4. For each downstream pipeline:
   a. Check intersection of changed_fields with consumed_fields
   b. Apply pipeline_mode
   c. Return decision (PROPAGATED/PAUSED/CONTINUED)
5. If all pipelines CONTINUED/PROPAGATED — allow registration
```

## Launch

```bash
cd schema-evolution-service
python -m uvicorn main:app --host 0.0.0.0 --port 8000
```
