with week_number as (

  select
    customer_id,
    format_date('%F', date_trunc(created_at,week(Sunday))) as week,
    total,
    row_number() over (partition by customer_id order by date_trunc(created_at,week(Sunday)) asc) as week_num
  from {{ref('staging_orders')}}
)
,

weekly_total as (

  select 
    customer_id,
    week,
    week_num,
    first_value(week) over (partition by customer_id order by week asc) as first_week,
    sum(total) as revenue
  from week_number
  group by 1,2,3

)
,

final as (

  select
    customer_id,
    first_week,
    week,
    week_num,
    revenue,
    sum(revenue) over (partition by customer_id order by week) as cumulative_revenue
  from weekly_total

)

select * from final