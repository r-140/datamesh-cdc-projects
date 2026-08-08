{{ config(
    materialized='table',
    schema='raw_silver'
) }}

SELECT
    order_id,
    customer_id,
    customer_email,
    status,
    total_amount,
    order_date
FROM {{ ref('brz_orders') }}