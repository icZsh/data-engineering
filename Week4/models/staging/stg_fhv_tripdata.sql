{{
    config(
        materialized='view'
    )
}}

select
    dispatching_base_num,
    affiliated_base_number,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    pulocationid as pickup_locationid,
    dolocationid as dropoff_locationid,
    cast(sr_flag as numeric) as sr_flag
from {{ source('staging', 'fhv_data') }}
where dispatching_base_num is not null