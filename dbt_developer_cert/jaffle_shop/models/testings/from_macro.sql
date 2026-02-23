{% set payment_methods = ['credit_card', 'coupon', 'gift_card', 'bank_transfer'] %}

select
    order_id,
    {{ generate_payment_pivot(payment_methods) }}
from {{ ref('stg_stripe__payments') }}
group by order_id