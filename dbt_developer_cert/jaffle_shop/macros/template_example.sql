{% macro template_example() %}
   {% set query %}
       SELECT true as BOOLEAN;
   {% endset %}
   {% set results = run_query(query) %}

   {% if execute %}
      {# Return the first column #}
      {% set results_list = results.columns[0].values() %}
   {% else %}
      {% set results_list = [] %}
   {% endif %}
   {{ log('The query result is ' ~ results_list, info=True)}}
   {% for result in results_list %}
       SELECT {{result}} as is_real {% if not loop.last%},{% endif %}
    {% endfor %}
{% endmacro %}