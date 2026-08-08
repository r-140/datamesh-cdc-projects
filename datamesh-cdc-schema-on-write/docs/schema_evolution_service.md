# Schema Evolution Service

## Назначение

REST-сервис, который:
1. Отслеживает изменения схем в Schema Registry
2. Оценивает влияние на downstream пайплайны
3. Принимает решение: PROPAGATE / PAUSE / CONTINUE

## API Endpoints

### POST /api/v1/schema/validate
Валидация новой схемы перед регистрацией.

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
Список пайплайнов и их статус.

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
Изменение режима подписки.

```json
{
  "mode": "opt-out"
}
```

## Логика работы

```
1. Получить новую схему от producer
2. Запросить текущую схему из Schema Registry
3. Сравнить diff (fields added/removed/type-changed)
4. Для каждого downstream pipeline:
   a. Проверить пересечение changed_fields с consumed_fields
   b. Применить pipeline_mode
   c. Вернуть решение (PROPAGATED/PAUSED/CONTINUED)
5. Если все пайплайны CONTINUED/PROPAGATED — разрешить регистрацию
```

## Запуск

```bash
cd schema-evolution-service
python -m uvicorn main:app --host 0.0.0.0 --port 8000
```
