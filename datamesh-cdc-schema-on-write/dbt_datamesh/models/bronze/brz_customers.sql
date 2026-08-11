{{ config(
    materialized='view',
    schema='raw_bronze'
) }}

SELECT
    id AS customer_id,
    full_name,
    email,
    COALESCE(country, 'US') AS country,
    to_timestamp(created_at / 1000000.0) AS created_at,
    to_timestamp(updated_at / 1000000.0) AS updated_at,
    __deleted
FROM {{ source('raw', 'customers_cdc') }}
