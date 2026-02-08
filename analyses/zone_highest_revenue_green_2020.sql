-- Zone with highest revenue for Green taxis in 2020.
-- Run after dbt compile: use the compiled SQL in target/compiled/.../analyses/
-- Or run: dbt compile && bq query --use_legacy_sql=false < target/compiled/.../analyses/zone_highest_revenue_green_2020.sql
with green_2020 as (
    select
        t.pickup_location_id,
        t.total_amount
    from {{ ref('fct_trips') }} t
    where t.service_type = 'Green'
      and extract(year from t.pickup_datetime) = 2020
),
revenue_by_zone as (
    select
        z.zone,
        z.borough,
        sum(g.total_amount) as total_revenue
    from green_2020 g
    join {{ ref('dim_zones') }} z on g.pickup_location_id = z.location_id
    group by z.zone, z.borough
)
select
    zone,
    borough,
    total_revenue
from revenue_by_zone
order by total_revenue desc
limit 1
