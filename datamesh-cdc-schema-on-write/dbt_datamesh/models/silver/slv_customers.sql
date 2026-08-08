{{ config(
    materialized='table',
    schema='raw_silver'
) }}

SELECT
    customer_id,
    customer_email,
    segment,
    registration_date
FROM {{ ref('brz_customers') }}