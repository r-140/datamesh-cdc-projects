{{ config(
    materialized='view',
    schema='raw_bronze'
) }}

SELECT
    id AS order_id,
    customer_id,
    customer_email,
    status,
    total_amount,
    order_date
FROM {{ source('raw', 'orders') }}