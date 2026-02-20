select
    count(*) as total_records
from {{ ref('fct_monthly_zone_revenue') }}
