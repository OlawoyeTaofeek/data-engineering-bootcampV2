{% macro safe_payment_type(column_name) %}

CASE
    WHEN {{ column_name }} IS NULL THEN NULL

    WHEN {{ column_name }}::TEXT ~ '^[0-9]+$'
    THEN
        CASE {{ column_name }}::INTEGER
            WHEN 1 THEN 'Credit card'
            WHEN 2 THEN 'Cash'
            WHEN 3 THEN 'No charge'
            ELSE 'Other'
        END

    ELSE NULL

END

{% endmacro %}
