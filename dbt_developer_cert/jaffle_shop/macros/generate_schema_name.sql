{% macro generate_schema_name(custom_schema_name, node) -%}

   {% if node.resource_type == 'snapshot' %}
       snapshots
   {% elif custom_schema_name is not none %}
       {{ custom_schema_name | trim }}
   {% else %}
       {{ target.schema }}
   {% endif %}

{%- endmacro %}