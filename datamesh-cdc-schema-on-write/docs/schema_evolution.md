# Schema Evolution

## Уровни защиты

### 1. Schema Registry (hard protection)
- Режим совместимости: **BACKWARD** (по умолчанию)
- Новая схема должна быть readable старыми consumers
- Breaking changes отклоняются с **HTTP 409**

### 2. Debezium ExtractNewRecordState (soft protection)
- `transforms.unwrap.delete.handling.mode=rewrite`
- `transforms.unwrap.drop.tombstones=false`
- При DROP COLUMN: заполняет `null` вместо падения

## Правила BACKWARD compatibility

| Операция | Результат | Правило |
|----------|-----------|---------|
| ADD COLUMN optional | ✅ Accepted | Новое поле с `default=null` |
| ADD COLUMN required | ❌ Rejected | Нет default — старые readers сломаются |
| DROP COLUMN | ⚠️ Debezium handles | Schema Registry 409, но Debezium null-fills |
| RENAME COLUMN | ❌ Rejected | Это удаление + добавление |
| CHANGE TYPE | ❌ Rejected | TYPE_MISMATCH |

## Проверка через API

```bash
# Регистрация новой схемы
curl -X POST http://localhost:8081/subjects/orders-value/versions   -H "Content-Type: application/vnd.schemaregistry.v1+json"   -d '{"schema": "{\"type\":\"record\"...}"}'

# Проверка совместимости
curl -X POST http://localhost:8081/compatibility/subjects/orders-value/versions/latest   -d '{"schema": "..."}'
```

## Переключение режима совместимости

```bash
# FULL — строже BACKWARD
curl -X PUT http://localhost:8081/config/orders-value   -d '{"compatibility": "FULL"}'
```
