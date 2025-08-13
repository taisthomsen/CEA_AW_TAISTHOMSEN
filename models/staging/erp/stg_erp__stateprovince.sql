{{
  config(
    materialized='view'
  )
}}

with source as (
    select * from {{ source('erp', 'stateprovince') }}
),

renamed as (
    select
        -- Primary Key
        stateprovinceid as state_province_id,
        
        -- State/Province Information
        stateprovincecode as state_province_code,
        countryregioncode as country_region_code,
        name as state_province_name,
        territoryid as territory_id,
        rowguid,
        modifieddate as modified_date
        
    from source
)

select * from renamed
