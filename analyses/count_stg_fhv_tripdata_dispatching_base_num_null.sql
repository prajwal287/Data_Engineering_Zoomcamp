-- Count of records in stg_fhv_tripdata where dispatching_base_num IS NULL.
-- Run after dbt compile; use compiled SQL from target/compiled/.../analyses/
select count(*) as record_count
from {{ ref('stg_fhv_tripdata') }}
where dispatching_base_num is null
