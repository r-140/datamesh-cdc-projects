{{ config(
    materialized='table',
    schema='raw_gold'
) }}

SELECT
    segment,
    COUNT(DISTINCT customer_id) AS customer_count,
    MIN(registration_date) AS earliest_registration,
    MAX(registration_date) AS latest_registration
FROM {{ ref('slv_customers') }}
GROUP BY segment