# Архитектура и симуляция проекта

## 1. Общая архитектура

Проект демонстрирует Data Mesh подход с CDC (Change Data Capture) и schema-on-write трансформациями.

### Стек технологий

| Компонент | Технология | Назначение |
|-----------|-----------|------------|
| Source DB | PostgreSQL (Debezium) | Доменные базы заказов и клиентов |
| CDC | Debezium + Kafka Connect | Захват изменений из WAL |
| Message Broker | Kafka (KRaft) | Потоковая передача событий |
| Schema Control | Confluent Schema Registry | Avro-схемы, compatibility checks |
| DWH | PostgreSQL | Целевая база для dbt |
| Transformations | dbt (dbt-postgres) | Bronze/Silver/Gold слои |
| Monitoring | Prometheus + Grafana | Метрики и дашборды |

### Data Flow

```
App → postgres-orders → WAL → Debezium → Kafka (Avro) → Schema Registry
                                                          ↓
                                                    postgres-dwh (seed)
                                                          ↓
                                               dbt: bronze → silver → gold
```

## 2. Слои данных

### Bronze (Views)
- `raw_bronze.brz_orders` — view над `raw.orders`
- `raw_bronze.brz_customers` — view над `raw.customers`

### Silver (Tables)
- `raw_silver.slv_orders` — очищенные заказы
- `raw_silver.slv_customers` — очищенные клиенты

### Gold (Aggregates)
- `raw_gold.fct_daily_revenue` — выручка по дням
- `raw_gold.dim_customer_segments` — сегменты клиентов

## 3. Schema Evolution

### Защита на трёх уровнях

1. **Schema Registry (hard)**
   - Режим BACKWARD по умолчанию
   - Breaking changes → HTTP 409

2. **Debezium (soft)**
   - ExtractNewRecordState заполняет null при DROP COLUMN
   - Коннектор не падает

3. **Pipeline Manager (business)**
   - Opt-in / Opt-out режимы
   - PAUSED / PROPAGATED / CONTINUED

### Сценарии симуляции

| Сценарий | Описание | Результат |
|----------|----------|-----------|
| A | Изменение типа (double→string) | PAUSED |
| B | Переименование поля | PAUSED |
| C | Добавление required поля | PAUSED |
| D | Добавление optional поля | PROPAGATED/CONTINUED |
| E | Nested record | PAUSED |
| F | Enum вместо string | PAUSED |
| G | Удаление поля (multi-pipeline) | Зависит от consumed fields |
| H | FULL compatibility | PAUSED |

## 4. Запуск симуляции

```bash
# 1. Инфраструктура
make up

# 2. CDC коннекторы
./scripts/setup-connectors.sh

# 3. End-to-end demo
python scripts/run_demo.py

# 4. Schema evolution simulator
python scripts/schema_evolution_simulator.py

# 5. DBT
make dbt-setup
make dbt-run
make dbt-test
```

## 5. Проверка результатов

```bash
# DWH
docker exec -it postgres-dwh psql -U dwh -d datamesh_dwh -c "SELECT * FROM raw_gold.fct_daily_revenue;"

# Kafka topics
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list

# Schema versions
curl http://localhost:8081/subjects/orders-server.public.orders-value/versions
```

## 6. Структура репозитория

```
.
├── docker-compose.yml          # Инфраструктура
├── Makefile                    # Команды
├── pyproject.toml              # Зависимости
├── README.md                   # Быстрый старт
├── .gitignore
├── kafka-connect/
│   └── Dockerfile              # Debezium Connect
├── postgres-dwh/
│   └── init.sql                # Seed-данные
├── scripts/
│   ├── setup-connectors.sh     # Регистрация CDC
│   ├── run_demo.py             # End-to-end demo
│   └── schema_evolution_simulator.py
├── dbt_datamesh/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   └── models/
│       ├── sources.yml
│       ├── bronze/
│       ├── silver/
│       ├── gold/
│       └── schema.yml
├── docs/                       # Документация
├── grafana/
│   └── provisioning/
└── prometheus/
```
