{% macro adapter_aware_clean_stale_models(
    database=target.database,
    schema=target.schema,
    days=1,
    dry_run=True,
    confirm=True,
    max_objects=50
) %}
    {{ return(adapter.dispatch('adapter_aware_clean_stale_models')(
        database=database,
        schema=schema,
        days=days,
        dry_run=dry_run,
        confirm=confirm,
        max_objects=max_objects
    )) }}
{% endmacro %}
 
{% macro default__adapter_aware_clean_stale_models(
    database, schema, days, dry_run, confirm, max_objects
) %}
    {{ log('Using Default Adapater', info=True)}}
    {% if not dry_run and not confirm %}
        {{ exceptions.raise_compiler_error(
            "Destructive action blocked. Set confirm=True to execute drops."
        ) }}
    {% endif %}

    {% set query %}
        select
            case when table_type = 'VIEW' then 'VIEW' else 'TABLE' end as object_type,
            'DROP ' ||
            case when table_type = 'VIEW' then 'VIEW' else 'TABLE' end ||
            ' "' || table_schema || '"."' || table_name || '";' as drop_statement
        from information_schema.tables
        where table_schema = '{{ schema }}'
        and table_name not like 'dbt_%'
        and table_name not like '%__dbt_tmp'
        and now() - interval '{{ days }} days' > current_timestamp
    {% endset %}

    {% if execute %}
        {% set results = run_query(query) %}
        {% set drop_queries = results.columns[1].values() %}
    {% else %}
        {% set drop_queries = [] %}
    {% endif %}

    {% if drop_queries | length > max_objects %}
        {{ exceptions.raise_compiler_error(
            "Safety stop: More than " ~ max_objects ~ " objects would be dropped."
        ) }}
    {% endif %}

    {% for stmt in drop_queries %}
        {% if dry_run %}
            {{ log("DRY RUN → " ~ stmt, info=True) }}
        {% else %}
            {{ log("Dropping → " ~ stmt, info=True) }}
            {% do run_query(stmt) %}
        {% endif %}
    {% endfor %}

{% endmacro %}

{% macro snowflake__adapter_aware_clean_stale_models(
    database, schema, days, dry_run, confirm, max_objects
) %}
    {{ log('Using Snowflake Adapater', info=True)}}
    {% if not dry_run and not confirm %}
        {{ exceptions.raise_compiler_error(
            "Destructive action blocked. Set confirm=True to execute drops."
        ) }}
    {% endif %}

    {% set query %}
        select
            table_type,
            'DROP ' ||
            case when table_type = 'VIEW' then 'VIEW' else 'TABLE' end ||
            ' {{ database }}.' || table_schema || '.' || table_name || ';'
        from {{ database }}.information_schema.tables
        where table_schema = upper('{{ schema }}')
        and table_name not like 'DBT_%'
        and table_name not like '%__DBT_TMP'
        and last_altered <= dateadd(day, -{{ days }}, current_timestamp())
    {% endset %}

    {% if execute %}
        {% set results = run_query(query) %}
        {% set drop_queries = results.columns[1].values() %}
    {% else %}
        {% set drop_queries = [] %}
    {% endif %}

    {% if drop_queries | length > max_objects %}
        {{ exceptions.raise_compiler_error(
            "Safety stop: More than " ~ max_objects ~ " objects would be dropped."
        ) }}
    {% endif %}

    {% for stmt in drop_queries %}
        {% if dry_run %}
            {{ log("DRY RUN → " ~ stmt, info=True) }}
        {% else %}
            {{ log("Dropping → " ~ stmt, info=True) }}
            {% do run_query(stmt) %}
        {% endif %}
    {% endfor %}

{% endmacro %}

{% macro bigquery__adapter_aware_clean_stale_models(
    database, schema, days, dry_run, confirm, max_objects
) %}
    {{ log('Using Bigquery Adapater', info=True)}}
    {% if not dry_run and not confirm %}
        {{ exceptions.raise_compiler_error(
            "Destructive action blocked. Set confirm=True to execute drops."
        ) }}
    {% endif %}

    {% set query %}
        select
            table_type,
            'DROP ' || table_type ||
            ' `' || table_schema || '.' || table_name || '`;' as drop_statement
        from `{{ database }}.{{ schema }}.INFORMATION_SCHEMA.TABLES`
        where table_name not like 'dbt_%'
        and table_name not like '%__dbt_tmp'
        and timestamp_millis(last_modified_time)
            <= timestamp_sub(current_timestamp(), interval {{ days }} day)
    {% endset %}

    {% if execute %}
        {% set results = run_query(query) %}
        {% set drop_queries = results.columns[1].values() %}
    {% else %}
        {% set drop_queries = [] %}
    {% endif %}

    {% if drop_queries | length > max_objects %}
        {{ exceptions.raise_compiler_error(
            "Safety stop: More than " ~ max_objects ~ " objects would be dropped."
        ) }}
    {% endif %}

    {% for stmt in drop_queries %}
        {% if dry_run %}
            {{ log("DRY RUN → " ~ stmt, info=True) }}
        {% else %}
            {{ log("Dropping → " ~ stmt, info=True) }}
            {% do run_query(stmt) %}
        {% endif %}
    {% endfor %}

{% endmacro %}