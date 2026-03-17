{% macro grant_select(schema=target.schema, role=target.role, database=target.database) %}
  {% set sql %}
      USE database {{ database }};
      GRANT usage on schema {{ schema }} to role {{ role }};
      GRANT SELECT ON ALL TABLES IN SCHEMA {{ schema }} to role {{ role }};
      GRANT SELECT ON ALL VIEWS IN SCHEMA {{ schema }} to role {{ role }};
  {% endset %}
  {{ log('Granting select on all tables and views in schema ' ~ target.schema ~ ' to role ' ~ role, info=True) }}
  {% do run_query(sql) %}
  {{ log('Privileges granted', info=True) }}
{% endmacro %}