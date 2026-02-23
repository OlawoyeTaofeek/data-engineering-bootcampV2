{% macro generate_payment_pivot(methods, amount_column) %}

    {% if methods | length == 0 %}
        {{ exceptions.raise_compiler_error("No payment methods provided") }}
    {% endif %}

    {% for method in methods %}
        sum(
            case 
                when payment_method = '{{ method }}' 
                then {{ amount_column }}
                else 0 
            end
        ) as {{ method }}_amount{% if not loop.last %},{% endif %}
    {% endfor %}

{% endmacro %}