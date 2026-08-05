-- Silver: deduplicated customers

SELECT DISTINCT
    id AS customer_id,
    email,
    full_name,
    country,
    created_at AS registered_at
FROM {{ ref('brz_customers') }}
WHERE email IS NOT NULL
