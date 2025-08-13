{{
  config(
    materialized='view'
  )
}}

with address as (
    select * from {{ ref('stg_erp__address') }}
),

state_province as (
    select * from {{ ref('stg_erp__stateprovince') }}
),

country_region as (
    select * from {{ ref('stg_erp__countryregion') }}
),

location_joined as (
    select
        a.address_id,
        a.address_line_1,
        a.address_line_2,
        a.city,
        a.state_province_id,
        sp.state_province_code,
        sp.state_province_name,
        sp.country_region_code,
        cr.country_region_name,
        a.postal_code,
        a.spatial_location,
        a.rowguid,
        a.modified_date
    from address a
    left join state_province sp on a.state_province_id = sp.state_province_id
    left join country_region cr on sp.country_region_code = cr.country_region_code
),

final as (
    select
        -- Primary Key
        address_id as location_key,
        
        -- Location Information
        city,
        state_province_name as state_province,
        country_region_name as country_region,
        state_province_code,
        country_region_code,
        postal_code,
        address_line_1,
        address_line_2,
        spatial_location,
        
        -- Metadata
        rowguid,
        modified_date
        
    from location_joined
)

select * from final
