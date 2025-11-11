with staged as (
    select * from {{ ref('st_health_data') }}
)

select
    drug,
    count(*) as total_events,
    max(ingest_ts) as last_ingest
from staged
group by drug
order by total_events desc
