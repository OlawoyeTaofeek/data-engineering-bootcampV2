{% for j in range(1, 10) %}
    SELECT {{ j }} as number,
           {{ j * j }} as square,
           {% if j % 2 == 0 -%} True as is_even
           {%- else -%} False as is_even
           {%- endif -%}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}

{#
{% for j in range(1, 10) -%}
SELECT {{ j }} as number,
       {{ j * j }} as square,
       {{ true if j % 2 == 0 else false }} as is_even
{% if not loop.last %}
UNION ALL
{% endif %}
{%- endfor %}
#}