{{
  config(
    materialized='table'
  )
}}

with sales_reason as (
    select * from {{ ref('stg_erp__salesreason') }}
),

final as (
    select
        sales_reason_id as sales_reason_key,
        reason_name,
        reason_type,
        modified_date,
        
        -- Metadata
        current_timestamp as dbt_updated_at,
        '{{ invocation_id }}' as dbt_run_id
        
    from sales_reason
)

select * from final
