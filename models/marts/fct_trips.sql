-- Fact table: one row per trip from unioned green + yellow taxi data.
-- BigQuery views require named columns; this selects from int_trips_unioned.
select
    vendor_id,
    rate_code_id,
    pickup_location_id,
    dropoff_location_id,
    pickup_datetime,
    dropoff_datetime,
    store_and_fwd_flag,
    passenger_count,
    trip_distance,
    trip_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    service_type
from {{ ref('int_trips_unioned') }}
