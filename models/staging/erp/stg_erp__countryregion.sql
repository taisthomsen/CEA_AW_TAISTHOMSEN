{{
  config(
    materialized='view'
  )
}}

with source as (
    select * from {{ source('erp', 'countryregion') }}
),

renamed as (
    select
        -- Primary Key
        countryregioncode as country_region_code,
        
        -- Country/Region Information
        name as country_region_name,
        modifieddate as modified_date
        
    from source
)

select * from renamed
