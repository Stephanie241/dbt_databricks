with raw as (
    select * from {{ ref('health_data_raw') }}
)

select
    safetyreportid,
    ingest_ts,
    patient_drug as drug,
    patient_reaction as reaction,
    receiver,
    duplicate
from raw
