{{ config(
    materialized = 'incremental',
    unique_key = 'safetyreportid'
) }}

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

{% if is_incremental() %}
-- Only process new data since the last ingest
where ingest_ts > (select max(ingest_ts) from {{ this }})
{% endif %}
