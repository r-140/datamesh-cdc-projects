-- Silver layer: schema-on-read with explicit CAST
-- Validated in CI against source schema before deployment
CREATE OR REPLACE VIEW silver.orders AS
SELECT
    _cdc_key AS id,
    _cdc_op,
    _cdc_ts_ms,
    CAST(json_extract_scalar(_payload, '$.customer_id') AS BIGINT) AS customer_id,
    CAST(json_extract_scalar(_payload, '$.total_amount') AS DOUBLE) AS total_amount,
    CAST(json_extract_scalar(_payload, '$.status') AS VARCHAR) AS status,
    CAST(json_extract_scalar(_payload, '$.promo_code') AS VARCHAR) AS promo_code,
    CAST(json_extract_scalar(_payload, '$.discount_pct') AS DOUBLE) AS discount_pct,
    _ingested_at
FROM bronze.orders
WHERE _cdc_op != 'd';
