{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

with payments as (

    select
        order_id,
        payment_method,
        {{ cents_to_dollars('amount', 4) }} as amount_usd
    from {{ ref('stg_stripe__payments') }}

),

aggregated as (

    select
        order_id,

        {{ generate_payment_pivot(payment_methods, 'amount_usd') }},

        sum(amount_usd) as total_amount_usd

    from payments
    group by order_id

)

select *
from aggregated

-- Variable-based threshold filter
where total_amount_usd >= {{ var('min_total_amount') }}

-- Environment-aware limit
{% if target.name == 'dev' %}
limit 100
{% endif %}