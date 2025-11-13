{{ config(
    materialized = 'incremental',
    unique_key = 'drug'
) }}

WITH staged AS (
    SELECT
        ingest_ts,
        regexp_extract(drug, 'medicinalproduct\': \'([^\']+)\'', 1) AS medicinalproduct
    FROM {{ ref('st_health_data') }}
)

SELECT
    COALESCE(medicinalproduct, 'Unknown') AS drug,
    COUNT(*) AS total_events,
    MAX(ingest_ts) AS last_ingest
FROM staged
{% if is_incremental() %}
WHERE ingest_ts > (SELECT MAX(last_ingest) FROM {{ this }})
{% endif %}
GROUP BY medicinalproduct
ORDER BY total_events DESC
