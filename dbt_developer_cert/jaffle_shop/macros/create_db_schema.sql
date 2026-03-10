{% macro ensure_database_and_schema(database, schema) %}

    {% if execute %}

        {{ log("Ensuring database exists → " ~ database, info=True) }}

        {% set create_db %}
            CREATE DATABASE IF NOT EXISTS {{ database }};
        {% endset %}

        {% do run_query(create_db) %}

        {{ log("Ensuring schema exists → " ~ database ~ "." ~ schema, info=True) }}

        {% set create_schema %}
            CREATE SCHEMA IF NOT EXISTS {{ database }}.{{ schema }};
        {% endset %}

        {% do run_query(create_schema) %}

    {% endif %}

{% endmacro %}