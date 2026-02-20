{#
   This macro returns the description of the payment type
#}

{% macro get_payment_type_description(column_name) -%}
    case {{column_name}}
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No Charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
    END 
{% endmacro %}