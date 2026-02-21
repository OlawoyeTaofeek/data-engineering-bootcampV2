{% set dbt_custom_arg_values = var('payment_type_values') %}

{{ config({"severity":"warn"}) }}
{{ test_accepted_values(column_name="payment_type", model=get_where_subquery(ref('stg_green_tripdata')), quote=false, values=dbt_custom_arg_values) }}