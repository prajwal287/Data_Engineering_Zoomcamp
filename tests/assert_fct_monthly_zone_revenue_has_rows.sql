-- Singular test: fails if fct_monthly_zone_revenue has zero rows.
-- Run with: dbt test --select test_type:singular
select *
from (
    select count(*) as row_count
    from {{ ref('fct_monthly_zone_revenue') }}
)
where row_count = 0
