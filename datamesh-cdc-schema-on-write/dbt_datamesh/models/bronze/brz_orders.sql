-- Bronze: raw CDC data from Kafka/Iceberg
-- Materialized as view — no transformation, just exposure

SELECT
    id,
    customer_id,
    CAST(total_amount AS DECIMAL(10,2)) AS total_amount,
    status,
    promo_code,
    created_at,
    updated_at
FROM {{ source('iceberg_raw', 'orders') }}
