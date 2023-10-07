with week_number as (

  select
    customer_id,
    format_date('%F', date_trunc(created_at,week(Sunday))) as week,
    total,
    row_number() over (partition by customer_id order by date_trunc(created_at,week(Sunday)) asc) as week_num
  from `analytics-engineers-club.coffee_shop.orders`

)
,

weekly_total as (

  select 
    customer_id,
    week,
    week_num,
    sum(total) as revenue,
  from week_number
  group by 1,2,3

)
,

final as (

  select
    customer_id,
    week,
    week_num,
    revenue,
    sum(revenue) over (partition by customer_id order by week) as cumulative_revenue
  from weekly_total

)

select * from cumulative_revenue_total