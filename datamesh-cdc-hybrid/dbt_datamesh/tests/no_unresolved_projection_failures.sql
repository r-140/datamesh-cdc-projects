{{ config(severity='warn') }}

select *
from {{ ref('projection_failures') }}
where not resolved_by_later_event
