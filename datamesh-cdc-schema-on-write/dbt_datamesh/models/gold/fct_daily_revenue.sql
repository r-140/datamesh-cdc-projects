-- Gold: daily revenue aggregation

SELECT
    DATE(created_at) AS order_date,
    COUNT(*) AS order_count,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(DISTINCT CASE WHEN has_promo THEN order_id END) AS promo_orders,
    country AS customer_country
FROM {{ ref('slv_orders') }}
GROUP BY DATE(created_at), country
