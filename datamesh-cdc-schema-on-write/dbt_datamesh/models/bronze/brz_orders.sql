{{ config(
    materialized='view',
    schema='raw_bronze'
) }}

SELECT
    id AS order_id,
    customer_id,
    total_amount::numeric(12,2) AS total_amount,
    status,
    to_timestamp(created_at / 1000000.0) AS created_at,
    to_timestamp(updated_at / 1000000.0) AS updated_at,
    __deleted
FROM {{ source('raw', 'orders_cdc') }}
