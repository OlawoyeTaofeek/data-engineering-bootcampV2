{% set allowed_statuses = ['completed', 'shipped'] %}

select *
from {{ ref('stg_jaffle_shop__orders') }}
where status in (
    {{ "'" ~ allowed_statuses | join("','") ~ "'" }}
)