-- Silver: cleaned orders with customer enrichment
-- Type casting, null handling, basic business logic

WITH cleaned_orders AS (
    SELECT
        id AS order_id,
        customer_id,
        total_amount,
        status,
        promo_code,
        -- Flag orders with promo
        CASE WHEN promo_code IS NOT NULL THEN TRUE ELSE FALSE END AS has_promo,
        created_at,
        updated_at,
        -- Partition key for Iceberg
        DATE(created_at) AS dt
    FROM {{ ref('brz_orders') }}
    WHERE total_amount IS NOT NULL  -- filter out broken records
)

SELECT
    o.*,
    c.email AS customer_email,
    c.country AS customer_country,
    c.full_name AS customer_name
FROM cleaned_orders o
LEFT JOIN {{ ref('brz_customers') }} c
    ON o.customer_id = c.id
