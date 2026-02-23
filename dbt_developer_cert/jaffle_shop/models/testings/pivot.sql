with successful_payment as (
    SELECT * FROM {{ref('stg_stripe__payments')}}
    WHERE status = 'success'
)
{% set payment_methods = var('pivot_statuses', ['credit_card', 'coupon', 'gift_card', 'bank_transfer']) %}

select

    {%- for payment_method in payment_methods %}
        sum(
            case 
                when payment_method = '{{ payment_method }}' 
                then amount 
                else 0 
            end
        ) as {{ payment_method }}_amount{% if not loop.last %},{% endif %}
    {% endfor %}

from successful_payment