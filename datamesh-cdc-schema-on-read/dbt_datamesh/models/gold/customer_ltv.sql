{{ config(materialized='table') }}

WITH customer_orders AS (
    SELECT
        c.id as customer_id,
        c.full_name,
        c.email,
        c.country,
        COUNT(o.id) as total_orders,
        COALESCE(SUM(o.total_amount), 0) as lifetime_value,
        MAX(o.created_at) as last_order_at
    FROM {{ ref('customers_silver') }} c
    LEFT JOIN {{ ref('orders_silver') }} o ON c.id = o.customer_id
    GROUP BY c.id, c.full_name, c.email, c.country
)
SELECT
    customer_id,
    full_name,
    email,
    country,
    total_orders,
    lifetime_value,
    CASE
        WHEN lifetime_value > 5000 THEN 'VIP'
        WHEN lifetime_value > 1000 THEN 'High'
        WHEN lifetime_value > 0 THEN 'Medium'
        ELSE 'New'
    END as customer_segment,
    last_order_at
FROM customer_orders
ORDER BY lifetime_value DESC
