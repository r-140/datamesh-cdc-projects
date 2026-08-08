# Grafana Monitoring Guide

## Доступ

- **URL:** http://localhost:3000
- **Login:** `admin`
- **Password:** `admin`

## Структура дашбордов

Все дашборды находятся в папке **Data Mesh** (Browse → Dashboards → Data Mesh).

---

## 1. CDC Pipeline Health

Мониторинг инфраструктуры CDC: Kafka Connect, Postgres DWH, состояние таблиц.

### Панели

#### Postgres DWH Connections
- **Тип:** Stat (число)
- **Метрика:** Активные подключения к `datamesh_dwh`
- **Thresholds:**
  - 🟢 < 10 — норма
  - 🟡 10–20 — внимание
  - 🔴 > 20 — перегрузка, возможен reject новых коннектов
- **Действие при 🔴:** Проверить `max_connections` в Postgres, найти висящие сессии

#### Postgres Transactions Rate
- **Тип:** График
- **Метрика:** `xact_commit` в секунду
- **Интерпретация:**
  - Плоская линия около 0 — DWH не нагружен (норма для пет-проекта)
  - Всплески — dbt run или CDC sink активен
  - Резкое падение до 0 — Postgres может быть недоступен

#### Connector Status
- **Тип:** Таблица
- **Содержимое:** Статус Debezium коннекторов
- **Ожидаемое:** `RUNNING` для обоих коннекторов
- **Если пусто:** Kafka Connect ещё не поднялся или коннекторы не зарегистрированы

#### Dead Tuples
- **Тип:** Stat
- **Метрика:** Мёртвые строки в таблицах (после UPDATE/DELETE)
- **Thresholds:**
  - 🟢 < 1000 — норма
  - 🟡 1000–10000 — запланировать VACUUM
  - 🔴 > 10000 — срочный VACUUM ANALYZE
- **Команда:** `docker exec postgres-dwh psql -U dwh -c "VACUUM ANALYZE;"`

---

## 2. Data Quality

Мониторинг слоёв данных: bronze → silver → gold.

### Панели

#### Row Counts by Layer
- **Тип:** Bar Gauge
- **Содержимое:** Количество строк в каждой таблице по слоям
- **Ожидаемое:**
  - `raw.orders` = 3 (seed)
  - `raw.customers` = 3 (seed)
  - `silver.orders` = 3 (после dbt run)
  - `silver.customers` = 3
  - `gold.daily_revenue` = N уникальных дат
  - `gold.segments` = 3 (premium, standard, basic)
- **Если 0 в silver/gold:** `dbt run` не выполнен или упал

#### Table Freshness
- **Тип:** Stat
- **Метрика:** Часы с последнего обновления данных
- **Thresholds:**
  - 🟢 < 24h — свежие данные
  - 🟡 24–72h — данные устаревают
  - 🔴 > 72h — критически устаревшие, проверить CDC pipeline
- **Для seed-данных:** Будет показывать много часов, т.к. даты фиксированы. Для production — обновлять `order_date` CURRENT_DATE.

#### Data Quality Tests
- **Тип:** Таблица
- **Содержимое:** Результаты dbt тестов
- **Ожидаемое:** Все `PASS`
- **Если FAIL:** Проверить `dbt test --select <test_name>`

#### Total Revenue Trend
- **Тип:** Time Series
- **Содержимое:** `SUM(total_amount)` по дням из `gold.fct_daily_revenue`
- **Интерпретация:**
  - Растущий тренд — бизнес растёт
  - Провалы — возможные проблемы с заказами или CDC лаг

#### Customer Segments
- **Тип:** Pie Chart
- **Содержимое:** Распределение клиентов по сегментам
- **Интерпретация:**
  - `premium` — высокая ценность
  - `standard` — основная масса
  - `basic` — потенциал для upsell

---

## Алерты (Prometheus)

Настроены в `prometheus/alerts.yml`.

| Алерт | Условие | Severity | Действие |
|-------|---------|----------|----------|
| `PostgresDWHDown` | exporter недоступен | critical | Проверить `docker compose ps postgres-dwh` |
| `PostgresHighConnections` | > 20 коннектов | warning | Найти висящие сессии, увеличить `max_connections` |
| `PostgresDeadTuplesHigh` | > 10000 мёртвых строк | warning | `VACUUM ANALYZE` |
| `KafkaConnectDown` | Connect недоступен | critical | Перезапустить `kafka-connect` |

### Просмотр алертов

1. Prometheus → Alerts: http://localhost:9090/alerts
2. Grafana → Alerting → Alert Rules

---

## Troubleshooting

### Дашборды пустые

```bash
# Проверить datasources
curl http://admin:admin@localhost:3000/api/datasources

# Перезагрузить provisioning
curl -X POST http://admin:admin@localhost:3000/api/admin/provisioning/dashboards/reload
```

### Postgres Exporter не даёт метрики

```bash
docker compose logs postgres-exporter --tail 20
curl http://localhost:9187/metrics | head
```

### Данные в Data Quality устарели

```bash
# Обновить seed-данные
docker exec -it postgres-dwh psql -U dwh -d datamesh_dwh
UPDATE raw.orders SET order_date = CURRENT_DATE;

# Перезапустить dbt
cd dbt_datamesh && dbt run
```
