{{ config(materialized='table') }}

WITH extracted AS (
    SELECT
        id,
        payload->>'name' as name,
        payload->>'full_name' as full_name,
        payload->>'email' as email,
        payload->>'country' as country,
        (payload->>'created_at')::timestamptz as created_at,
        (payload->>'updated_at')::timestamptz as updated_at,
        payload->>'__deleted' as __deleted,
        __op,
        __source_ts_ms,
        ingested_at
    FROM {{ ref('customers_bronze') }}
)
SELECT *
FROM extracted
WHERE __deleted IS DISTINCT FROM 'true'
