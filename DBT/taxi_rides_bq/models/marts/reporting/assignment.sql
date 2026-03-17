select
    pickup_zone,
    sum(revenue_monthly_total_amount) as total_revenue_2020
from {{ ref('fct_monthly_zone_revenue') }}
where service_type = 'Green'
  and extract(year from revenue_month) = 2020
group by pickup_zone
order by total_revenue_2020 desc
limit 1
