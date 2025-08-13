{{
  config(
    materialized='table'
  )
}}

with date as (
    select * from {{ ref('int_erp__dim_date') }}
),

final as (
    select
        date_key,
        full_date,
        day_of_week,
        day_name,
        day_of_month,
        day_of_year,
        week_of_year,
        month_number,
        month_name,
        quarter,
        year,
        is_weekend,
        is_christmas,
        is_independence_day,
        fiscal_year,
        fiscal_month,
        
        -- Metadata
        current_timestamp as dbt_updated_at,
        '{{ invocation_id }}' as dbt_run_id
        
    from date
)

select * from final
