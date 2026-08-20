with failures as (
    select * from {{ source('governance', 'projection_failures') }}
),
promoted_entities as (
    select 'orders' as source_table, order_id as entity_id, bronze_offset
    from {{ ref('stg_orders') }}
    union all
    select 'customers' as source_table, customer_id as entity_id, bronze_offset
    from {{ ref('stg_customers') }}
)
select
    f.topic,
    f.kafka_partition,
    f.kafka_offset,
    f.source_table,
    f.payload,
    f.error,
    f.failed_at,
    exists (
        select 1
        from promoted_entities p
        where p.source_table = f.source_table
          and p.entity_id = case
              when f.payload ->> 'id' ~ '^-?[0-9]+$'
              then (f.payload ->> 'id')::bigint
          end
          and p.bronze_offset > f.kafka_offset
    ) as resolved_by_later_event
from failures f
