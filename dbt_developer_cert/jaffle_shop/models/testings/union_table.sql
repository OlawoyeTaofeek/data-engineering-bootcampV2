{{ union_tables_by_prefix(
    database=target.database, 
    schema=this.schema,
    prefix='orders__'
) }}