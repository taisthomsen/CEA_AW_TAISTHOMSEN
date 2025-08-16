{{
  config(
    materialized='view'
  )
}}

with person as (
    select * from {{ ref('stg_erp__person') }}
),

customer_filtered as (
    select *
    from person
    where person_type in ('SC', 'VC', 'IN', 'GC')  -- Customer types
),

final as (
    select
        -- Primary Key
        business_entity_id as customer_key,
        
        -- Customer Information
        concat(first_name, ' ', last_name) as customer_name,
        person_type as customer_type,
        first_name,
        last_name,
        title,
        suffix,
        email_promotion,
        
        -- Metadata
        rowguid,
        modified_date
        
    from customer_filtered
)

select * from final
