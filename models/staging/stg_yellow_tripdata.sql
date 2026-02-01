select *
from {{ source('raw_data', 'yellow_tripdata_partitioned') }}