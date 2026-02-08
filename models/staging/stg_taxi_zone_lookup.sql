-- Staging model for NYC taxi zone lookup table.
-- Source table must exist in BigQuery (e.g. from ingestion or loaded from CSV).
-- Output view is stg_taxi_zone_lookup so it does not conflict with existing table taxi_zone_lookup.
select
    locationid,
    borough,
    zone,
    service_zone
from {{ source('raw_data', 'taxi_zone_lookup') }}
