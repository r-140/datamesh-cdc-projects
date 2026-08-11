{{ config(
    materialized='table',
    schema='raw_silver'
) }}

SELECT
    customer_id,
    full_name,
    email,
    country,
    created_at,
    updated_at
FROM {{ ref('brz_customers') }}
WHERE __deleted IS NULL
  OR __deleted = 'false'
