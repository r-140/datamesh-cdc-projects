{{ config(
    materialized='table',
    schema='raw_silver'
) }}

SELECT
    order_id,
    customer_id,
    total_amount,
    status,
    created_at,
    updated_at
FROM {{ ref('brz_orders') }}
WHERE __deleted IS NULL
  OR __deleted = 'false'
