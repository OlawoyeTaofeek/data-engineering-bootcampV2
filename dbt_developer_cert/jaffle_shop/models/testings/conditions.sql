select 
   order_id,
   amount,
   {%- if amount > 0 -%} 'positive' 
   {%- elif amount < 0 -%} 'negative' 
   {%- else -%} 'zero' 
   {%- endif -%} as amount_type,

   {% if var('apply_discount', true)%}
    amount * 0.9 as discounted_amount
    {% else %}
    amount as discounted_amount
    {% endif %}
from {{ ref('stg_stripe__payments') }}