{% macro safe_cast(column_name, type) %}
    {% if target.type == 'bigquery' %}
        safe_cast({{ column_name }} as {{ type }})
    {% else %}
        CASE
            WHEN {{ column_name }} IS NOT NULL AND {{ column_name }}::TEXT ~ '^[0-9]+$'
            THEN {{ column_name }}::{{ type }}
            ELSE NULL
        END
    {% endif %}
{% endmacro %}
