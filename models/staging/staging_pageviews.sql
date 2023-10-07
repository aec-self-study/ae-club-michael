with customer_unique_id as (

  select 
    customer_id,
    visitor_id,
    row_number() over (partition by customer_id order by visitor_id asc) as id_row_number
  from `analytics-engineers-club.web_tracking.pageviews`
)
,

standard_customer_id as (

  select 
    m.id as id,
    c.visitor_id as visitor_id,
    m.device_type as device_type,
    m.timestamp as timestamp,
    m.page as page,
    m.customer_id as customer_id
  from `analytics-engineers-club.web_tracking.pageviews` m
  left join customer_unique_id c on m.customer_id = c.customer_id
  where c.id_row_number = 1

)
,

time_difference as (

  select
    *,
    date_diff(timestamp,lag(timestamp) over (partition by visitor_id order by timestamp),minute) as time_difference
  from standard_customer_id

)
,

new_session as (

  select
    d.*,
    case
      when d.time_difference is null then 1
      when d.time_difference >= 30 then 1
      else 0
    end as new_session
  from time_difference d

)
,

sessionization as (

  select
    s.*,
    substr(visitor_id, 0,10) || '_' || sum(new_session) over (partition by visitor_id order by timestamp) as session_id
  from new_session s

)
,

final as (

  select
    id,
    visitor_id,
    customer_id,
    device_type,
    timestamp,
    page,
    session_id
  from sessionization 

)

select * from final