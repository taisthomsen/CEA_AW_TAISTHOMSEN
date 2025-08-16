{{
  config(
    materialized='table'
  )
}}

with salesperson as (
    select * from {{ ref('int_erp__dim_salesperson') }}
),

final as (
    select
        salesperson_key,
        salesperson_name,
        employee_type,
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
        
    from salesperson
)

select * from final
