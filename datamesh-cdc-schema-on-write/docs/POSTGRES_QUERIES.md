# Полезные запросы (PostgreSQL DWH)

## Подключение

```bash
# DWH
docker exec -it postgres-dwh psql -U dwh -d datamesh_dwh

# Source databases
docker exec -it postgres-orders psql -U postgres -d orders_db
docker exec -it postgres-customers psql -U postgres -d customers_db
```

## Проверка схем DWH

```sql
-- Список схем
\dn

-- Таблицы в raw
\dt raw.*

-- Таблицы в bronze
\dt raw_bronze.*

-- Таблицы в silver
\dt raw_silver.*

-- Таблицы в gold
\dt raw_gold.*
```

## Seed-данные

```sql
SELECT * FROM raw.orders;
SELECT * FROM raw.customers;
```

## Bronze layer

```sql
-- Представления над source
SELECT * FROM raw_bronze.brz_orders LIMIT 5;
SELECT * FROM raw_bronze.brz_customers LIMIT 5;
```

## Silver layer

```sql
-- Очищенные данные
SELECT 
    order_id,
    customer_id,
    status,
    total_amount,
    order_date
FROM raw_silver.slv_orders
WHERE total_amount > 0;
```

## Gold layer

```sql
-- Ежедневная выручка
SELECT * FROM raw_gold.fct_daily_revenue ORDER BY order_date;

-- Сегменты клиентов
SELECT * FROM raw_gold.dim_customer_segments;
```

## CDC-проверка (source)

```sql
-- Проверка WAL-level
SHOW wal_level;  -- должно быть 'logical'

-- Список publication
SELECT * FROM pg_publication;

-- Репликационные слоты
SELECT * FROM pg_replication_slots;
```

## Kafka Connect

```bash
# Статус коннекторов
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/orders-cdc-connector/status
```

## Schema Registry

```bash
# Субъекты
curl http://localhost:8081/subjects

# Версии схемы
curl http://localhost:8081/subjects/orders-server.public.orders-value/versions

# Конкретная версия
curl http://localhost:8081/subjects/orders-server.public.orders-value/versions/1
```
