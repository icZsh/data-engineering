{{
    config(
        materialized='table'
    )
}}

with quarterly_revenues as (
    select
        service_type,
        extract(year from pickup_datetime) as year,
        extract(quarter from pickup_datetime) as quarter,
        sum(total_amount) as quarterly_revenue
    from {{ ref('fact_trips') }}
    group by 1, 2, 3
),
lagged_revenues as (
    select 
        *,
        lag(quarterly_revenue) over (
            partition by service_type, quarter 
            order by year
        ) as prev_year_revenue
    from quarterly_revenues
)
select
    service_type,
    year,
    quarter,
    concat(cast(year as string), '-Q', cast(quarter as string)) as year_quarter,
    quarterly_revenue,
    prev_year_revenue,
    -- Calculate YoY growth as percentage
    case 
        when prev_year_revenue is not null and prev_year_revenue != 0
        then round(((quarterly_revenue - prev_year_revenue) / prev_year_revenue) * 100, 2)
        else null 
    end as revenue_growth_yoy_pct
from lagged_revenues
order by service_type, year, quarter