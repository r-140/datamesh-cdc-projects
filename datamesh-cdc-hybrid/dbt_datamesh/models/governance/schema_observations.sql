select
    source_table,
    fingerprint,
    fields,
    event_count,
    first_seen_at,
    last_seen_at
from {{ source('governance', 'observed_schemas') }}
