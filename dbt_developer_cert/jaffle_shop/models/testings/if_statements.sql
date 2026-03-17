{%- set temperature = 80.0 -%}

SELECT 'A day like this, I especially like'
{%- if temperature > 75 -%} 'to be outside and enjoy the warm weather.'

{%- else -%}'to be inside with a cozy drink.'

{%- endif -%}