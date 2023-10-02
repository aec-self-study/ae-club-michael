with customer_unique_id as (
  select 
    customer_id,
    visitor_id,
    row_number() over (partition by customer_id order by visitor_id asc) as id_row_number
  from `analytics-engineers-club.web_tracking.pageviews`
  where customer_id is not null
)

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