select
    id as order_id,
    customer_id,
    total_amount,
    status,
    bronze_topic,
    bronze_partition,
    bronze_offset,
    updated_at
from {{ source('silver', 'orders') }}
