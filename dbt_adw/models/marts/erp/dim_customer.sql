{{
  config(
    materialized='table'
  )
}}

with customer as (
    select * from {{ ref('int_erp__dim_customer') }}
),

final as (
    select
        customer_key,
        customer_name,
        customer_type,
        first_name,
        last_name,
        title,
        suffix,
        email_promotion,
        rowguid,
        modified_date,
        
        -- Metadata
        current_timestamp as dbt_updated_at,
        '{{ invocation_id }}' as dbt_run_id
        
    from customer
)

select * from final
