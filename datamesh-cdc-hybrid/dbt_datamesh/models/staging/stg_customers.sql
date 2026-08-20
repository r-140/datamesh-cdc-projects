select
    id as customer_id,
    email,
    full_name,
    country,
    bronze_topic,
    bronze_partition,
    bronze_offset,
    updated_at
from {{ source('silver', 'customers') }}
