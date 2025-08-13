{{
  config(
    materialized='view'
  )
}}

with person as (
    select * from {{ ref('stg_erp__person') }}
),

salesperson_filtered as (
    select *
    from person
    where person_type in ('EM', 'SP')  -- Employee and Sales Person types
),

final as (
    select
        -- Primary Key
        business_entity_id as salesperson_key,
        
        -- Salesperson Information
        concat(first_name, ' ', last_name) as salesperson_name,
        person_type as employee_type,
        first_name,
        last_name,
        title,
        suffix,
        email_promotion,
        
        -- Metadata
        rowguid,
        modified_date
        
    from salesperson_filtered
)

select * from final
