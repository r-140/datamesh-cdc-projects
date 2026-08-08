{{ config(
    materialized='table',
    schema='raw_gold'
) }}

SELECT
    order_date,
    COUNT(*) AS order_count,
    SUM(total_amount) AS total_revenue
FROM {{ ref('slv_orders') }}
GROUP BY order_date