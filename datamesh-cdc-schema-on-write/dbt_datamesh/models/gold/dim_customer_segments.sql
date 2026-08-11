{{ config(
    materialized='table',
    schema='raw_gold'
) }}

SELECT
    country AS segment,
    COUNT(*) AS customer_count,
    MIN(DATE(created_at)) AS earliest_registration,
    MAX(DATE(created_at)) AS latest_registration
FROM {{ ref('slv_customers') }}
GROUP BY country
