{{ config(materialized='table') }}

WITH extracted AS (
    SELECT
        id,
        (payload->>'customer_id')::bigint as customer_id,
        (payload->>'total_amount')::numeric(12,2) as total_amount,
        payload->>'status' as status,
        (payload->>'created_at')::timestamptz as created_at,
        (payload->>'updated_at')::timestamptz as updated_at,
        payload->>'__deleted' as __deleted,
        __op,
        __source_ts_ms,
        ingested_at
    FROM {{ ref('orders_bronze') }}
)
SELECT *
FROM extracted
WHERE __deleted IS DISTINCT FROM 'true'
