{% set database = target.database %}
{% set schema = target.schema %}

SELECT table_type, 
       table_schema, 
       table_name,
       last_altered,
       CASE 
           WHEN table_type = 'VIEW' 
               THEN table_type 
           else 
               'TABLE' 
        END AS object_type,
        'DROP ' || object_type || ' {{ database | upper}}.' || table_schema || '.' || table_name || ';' as drop_statement
FROM {{ database }}.information_schema.tables
WHERE table_schema = UPPER('{{ schema }}')
   AND date(last_altered) <= date(dateadd('day', -7, current_date))
ORDER BY 4 desc