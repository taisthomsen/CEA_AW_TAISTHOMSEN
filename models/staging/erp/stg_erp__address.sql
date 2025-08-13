{{
  config(
    materialized='view'
  )
}}

with source as (
    select * from {{ source('erp', 'address') }}
),

renamed as (
    select
        -- Primary Key
        addressid as address_id,
        
        -- Address Information
        addressline1 as address_line_1,
        addressline2 as address_line_2,
        city,
        stateprovinceid as state_province_id,
        postalcode as postal_code,
        spatiallocation as spatial_location,
        rowguid,
        modifieddate as modified_date
        
    from source
)

select * from renamed
