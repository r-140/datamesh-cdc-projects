{{ config(materialized='table') }}

WITH daily AS (
    SELECT
        DATE(created_at) as order_date,
        status,
        COUNT(*) as order_count,
        SUM(total_amount) as total_revenue,
        AVG(total_amount) as avg_order_value
    FROM {{ ref('orders_silver') }}
    GROUP BY DATE(created_at), status
)
SELECT
    order_date,
    status,
    order_count,
    total_revenue,
    avg_order_value
FROM daily
ORDER BY order_date DESC, status
