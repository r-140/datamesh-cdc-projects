# Pipeline Manager

## Концепция

Каждый downstream-пайплайн (consumer) регистрирует свой **режим подписки** на домен:

| Режим | Описание | Поведение при breaking change |
|-------|----------|------------------------------|
| **opt-in** | Явное согласие на все изменения | PROPAGATED — изменения применяются |
| **opt-out** | Явный отказ от breaking changes | PAUSED — пайплайн останавливается |

## Логика принятия решений

```python
def evaluate_pipeline(pipeline_mode, consumed_fields, changed_fields, change_type):
    """
    pipeline_mode: 'opt-in' | 'opt-out'
    consumed_fields: ['status', 'total_amount']
    changed_fields: ['status']  # удалено
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

## Примеры сценариев

### Сценарий A: Изменение типа поля (double → string)
- **orders-to-analytics** (opt-in) → PAUSED (incompatible schema)
- **orders-to-reporting** (opt-out) → PAUSED (incompatible schema)

### Сценарий D: Добавление optional поля (promo_code)
- **orders-to-analytics** (opt-in) → PROPAGATED
- **orders-to-reporting** (opt-out) → CONTINUED (changes outside consumed fields)

### Сценарий G: Удаление поля status
- **orders-to-ml** (opt-out, consumes `status`) → PAUSED
- **orders-to-bi** (opt-out, не использует `status`) → CONTINUED
- **orders-to-archive** (opt-in) → PROPAGATED

## Schema Registry Integration

Pipeline Manager проверяет регистрацию схемы перед принятием решения:
- Если Schema Registry вернул **409** — изменение breaking
- Если **200** — изменение compatible
