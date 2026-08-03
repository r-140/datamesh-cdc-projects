CREATE OR REPLACE VIEW silver.customers AS
SELECT
    _cdc_key AS id,
    _cdc_op,
    _cdc_ts_ms,
    CAST(json_extract_scalar(_payload, '$.email') AS VARCHAR) AS email,
    CAST(json_extract_scalar(_payload, '$.full_name') AS VARCHAR) AS full_name,
    CAST(json_extract_scalar(_payload, '$.country') AS VARCHAR) AS country,
    _ingested_at
FROM bronze.customers
WHERE _cdc_op != 'd';
