{% macro union_tables_by_prefix(database, schema, prefix) %}

  {% set tables = dbt_utils.get_relations_by_prefix(
        database=database,
        schema=schema,
        prefix=prefix
    ) %}

  {% set filtered_tables = tables 
        | rejectattr('identifier', 'equalto', 'orders_snapshot') 
        | list %}

  {% for table in filtered_tables %}

    {% if not loop.first %}
UNION ALL
    {% endif %}

SELECT *
FROM {{ table }}

  {% endfor %}

{% endmacro %}