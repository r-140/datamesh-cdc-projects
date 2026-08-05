-- Gold: customer segmentation

SELECT
    customer_id,
    customer_email,
    customer_country,
    COUNT(order_id) AS total_orders,
    SUM(total_amount) AS lifetime_value,
    AVG(total_amount) AS avg_order_value,
    MAX(created_at) AS last_order_date,
    -- Simple segmentation
    CASE
        WHEN SUM(total_amount) > 500 THEN 'VIP'
        WHEN SUM(total_amount) > 100 THEN 'Regular'
        ELSE 'New'
    END AS segment
FROM {{ ref('slv_orders') }}
GROUP BY customer_id, customer_email, customer_country
