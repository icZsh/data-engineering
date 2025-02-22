{{
    config(
        materialized='table'
    )
}}

with valid_trips as (
    select
        service_type,
        extract(year from pickup_datetime) as year,
        extract(month from pickup_datetime) as month,
        fare_amount
    from {{ ref('fact_trips') }}
    where 
        fare_amount > 0 
        and trip_distance > 0
)
select
    distinct service_type,
    year,
    month,
    percentile_cont(fare_amount, 0.90) over (
        partition by service_type, year, month
    ) as fare_amount_p90,
    percentile_cont(fare_amount, 0.95) over (
        partition by service_type, year, month
    ) as fare_amount_p95,
    percentile_cont(fare_amount, 0.97) over (
        partition by service_type, year, month
    ) as fare_amount_p97
from valid_trips
order by service_type, year, month