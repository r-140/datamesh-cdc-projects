# Архитектура проекта

## Общая схема

```
┌─────────────────┐     ┌─────────────┐     ┌─────────────────┐
│  postgres-orders│     │             │     │  postgres-dwh   │
│  (source) :5432 │────▶│  Debezium   │────▶│  (DWH) :5434    │
│                 │     │  Connector  │     │                 │
└─────────────────┘     └──────┬──────┘     └─────────────────┘
                               │
┌─────────────────┐           │           ┌─────────────────┐
│ postgres-customers│         │           │  dbt models     │
│  (source) :5433 │───────────┘           │  bronze/silver/ │
│                 │                       │  gold           │
└─────────────────┘                       └─────────────────┘
                               │
                        ┌──────┴──────┐
                        │    Kafka    │
                        │  (KRaft)    │
                        │   :9092     │
                        └──────┬──────┘
                               │
                        ┌──────┴──────┐
                        │Schema Registry│
                        │   :8081     │
                        └─────────────┘
```

## Компоненты

### Source Layer
- **postgres-orders** (:5432) — домен заказов, WAL включён для Debezium
- **postgres-customers** (:5433) — домен клиентов

### Streaming Layer
- **Kafka (KRaft)** (:9092) — брокер сообщений без Zookeeper
- **Schema Registry** (:8081) — Avro-схемы, BACKWARD compatibility
- **Kafka Connect** (:8083) — Debezium Source Connectors

### Warehouse Layer
- **postgres-dwh** (:5434) — целевая БД для dbt
  - `raw` — seed-данные
  - `raw_bronze` — views (бронза)
  - `raw_silver` — очищенные таблицы
  - `raw_gold` — агрегаты

### Transformations
- **dbt** — модели bronze → silver → gold
- **dbt-postgres** — адаптер для Postgres DWH

### Monitoring
- **Prometheus** (:9090) — метрики
- **Grafana** (:3000) — дашборды

## Data Flow

1. Приложение пишет в `postgres-orders` / `postgres-customers`
2. Debezium читает WAL и публикует события в Kafka (Avro)
3. Schema Registry валидирует схемы (BACKWARD compat)
4. Данные доступны в DWH через seed / CDC sink (опционально)
5. dbt строит bronze/silver/gold слои
