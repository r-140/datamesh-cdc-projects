{{ config(
    materialized='table',
    schema='raw_gold'
) }}

SELECT
    DATE(created_at) AS order_date,
    COUNT(*) AS order_count,
    SUM(total_amount) AS total_revenue
FROM {{ ref('slv_orders') }}
GROUP BY DATE(created_at)
