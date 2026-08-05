-- Bronze: raw CDC data from Kafka/Iceberg

SELECT
    id,
    email,
    full_name,
    country,
    created_at
FROM {{ source('iceberg_raw', 'customers') }}
