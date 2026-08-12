{{ config(materialized='view') }}

SELECT
    id,
    payload,
    __op,
    __source_ts_ms,
    __kafka_partition,
    __kafka_offset,
    ingested_at
FROM {{ source('raw', 'orders_cdc') }}
