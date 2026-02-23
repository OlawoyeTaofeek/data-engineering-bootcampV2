{% set statuses = ['success', 'fail'] %}

select
    {%- for status in statuses %}
       sum(case when status = '{{ status }}' then 1 end) as {{ status }}_orders
       {% if not loop.last %},
       {% endif %}
    {%- endfor -%}
from {{ ref('stg_stripe__payments') }}