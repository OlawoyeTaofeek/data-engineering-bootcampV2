{{ 
    config(
        {'schema': 'staging'}
    )
}}

{{ union_tables_by_prefix(
    database=target.database, 
    schema = target.schema,
    prefix='orders__' 
    ) 
}}