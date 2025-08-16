{{
  config(
    materialized='view'
  )
}}

with source as (
    select * from {{ source('erp', 'salesreasons') }}
),

renamed as (
    select
        -- Primary Key
        salesreasonid as sales_reason_id,
        
        -- Sales Reason Information
        name as reason_name,
        reasontype as reason_type,
        modifieddate as modified_date
        
    from source
)

select * from renamed
