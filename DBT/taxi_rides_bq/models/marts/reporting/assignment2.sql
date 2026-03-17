select
    sum(total_monthly_trips) as total_trips_oct_2019
from {{ ref('fct_monthly_zone_revenue') }}
where service_type = 'Green'
  and revenue_month BETWEEN DATE '2019-10-01' AND DATE '2019-10-31'   