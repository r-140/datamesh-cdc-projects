{{ config(
    materialized='view',
    schema='raw_bronze'
) }}

SELECT
    id AS customer_id,
    email AS customer_email,
    segment,
    registration_date
FROM {{ source('raw', 'customers') }}