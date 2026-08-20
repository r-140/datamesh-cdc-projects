select
    country,
    count(*) as customer_count,
    max(updated_at) as last_customer_update
from {{ ref('stg_customers') }}
group by country
