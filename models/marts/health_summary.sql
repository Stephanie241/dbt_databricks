{{ config(
    materialized = 'incremental',
    unique_key = 'drug'
) }}

with staged as (
    select * from {{ ref('st_health_data') }}
)

select
    drug,
    count(*) as total_events,
    max(ingest_ts) as last_ingest
from staged
{% if is_incremental() %}
where ingest_ts > (select max(last_ingest) from {{ this }})
{% endif %}
group by drug
order by total_events desc
