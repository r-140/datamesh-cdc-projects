select
    status,
    count(*) as order_count,
    sum(total_amount) as total_revenue,
    avg(total_amount) as average_order_value,
    max(updated_at) as last_order_update
from {{ ref('stg_orders') }}
group by status
