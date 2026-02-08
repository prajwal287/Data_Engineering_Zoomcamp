-- Staging model for NYC For-Hire Vehicle (FHV) trip data.
-- Requires: table raw_data.fhv_tripdata in BigQuery (e.g. psyched-loader-485321-a8.zoomcamp.fhv_tripdata).
-- If the table is missing, run: dbt run --exclude stg_fhv_tripdata
-- If your table has a different name/dataset, update the source in models/staging/bigquery_sources.yml.
select
    dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    cast(pulocationid as integer) as pickup_location_id,
    cast(dolocationid as integer) as dropoff_location_id,
    sr_flag,
    affiliated_base_num
from {{ source('raw_data', 'fhv_tripdata') }}
