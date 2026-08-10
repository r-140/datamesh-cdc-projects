# Pipeline Manager

## Concept

Each downstream pipeline (consumer) registers its **subscription mode** for a domain:

| Mode | Description | Behavior on breaking change |
|------|-------------|-----------------------------|
| **opt-in** | Explicit consent to all changes | PROPAGATED — changes are applied |
| **opt-out** | Explicit refusal of breaking changes | PAUSED — pipeline is stopped |

## Decision Logic

```python
def evaluate_pipeline(pipeline_mode, consumed_fields, changed_fields, change_type):
    """
    pipeline_mode: 'opt-in' | 'opt-out'
    consumed_fields: ['status', 'total_amount']
    changed_fields: ['status']  # removed
    change_type: 'BREAKING' | 'COMPATIBLE'
    """
    if change_type == 'COMPATIBLE':
        return 'CONTINUED'

    if pipeline_mode == 'opt-in':
        return 'PROPAGATED'

    # opt-out + breaking
    affected = set(consumed_fields) & set(changed_fields)
    if affected:
        return 'PAUSED'
    return 'CONTINUED'
```

## Scenario Examples

### Scenario A: Type change (double -> string)
- **orders-to-analytics** (opt-in) -> PAUSED (incompatible schema)
- **orders-to-reporting** (opt-out) -> PAUSED (incompatible schema)

### Scenario D: Add optional field (promo_code)
- **orders-to-analytics** (opt-in) -> PROPAGATED
- **orders-to-reporting** (opt-out) -> CONTINUED (changes outside consumed fields)

### Scenario G: Delete status field
- **orders-to-ml** (opt-out, consumes `status`) -> PAUSED
- **orders-to-bi** (opt-out, does not use `status`) -> CONTINUED
- **orders-to-archive** (opt-in) -> PROPAGATED

## Schema Registry Integration

Pipeline Manager checks schema registration before making a decision:
- If Schema Registry returns **409** — change is breaking
- If **200** — change is compatible
