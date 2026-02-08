-- One row per month per pickup zone: total revenue and trip count.
-- Use for reporting and dashboards (e.g. monthly revenue by zone).
with trips as (
    select
        date_trunc(t.pickup_datetime, month) as revenue_month,
        t.pickup_location_id,
        t.total_amount
    from {{ ref('fct_trips') }} t
),
zones as (
    select location_id, zone, borough
    from {{ ref('dim_zones') }}
)
select
    trips.revenue_month,
    trips.pickup_location_id,
    zones.zone,
    zones.borough,
    count(*) as trip_count,
    sum(trips.total_amount) as total_revenue
from trips
left join zones on trips.pickup_location_id = zones.location_id
group by 1, 2, 3, 4
