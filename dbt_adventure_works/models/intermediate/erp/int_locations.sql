
with 
    addresses as (
        select * 
        from {{ ref('stg_erp__address') }}
    )

    , state_provinces as (
        select * 
        from {{ ref('stg_erp__stateprovince') }}
    )

    , country_regions as (
        select * 
        from {{ ref('stg_erp__countryregion') }}
    )

    , location_joined as (
        select
            addresses.address_id
            , addresses.city_name
            , addresses.address_line_1
            , addresses.address_line_2
            , addresses.state_province_id
            , addresses.postal_code
            , addresses.spatial_location_address
            , addresses.last_updated_at as source_last_updated_at
            , state_provinces.country_region_id
            , state_provinces.territory_id
            , state_provinces.is_state_equal_country_flag
            , state_provinces.state_province_name
            , state_provinces.state_province_code
            , country_regions.country_region_name
        from addresses
        left join state_provinces   
            on addresses.state_province_id = state_provinces.state_province_id
        left join country_regions
            on state_provinces.country_region_id = country_regions.country_region_id
)

    , transformed as (
        select
            -- Primary Key
            address_id   
            -- Location Information
            , city_name
            , address_line_1
            , address_line_2
            , territory_id
            , state_province_name
            , state_province_id
            , country_region_name
            , country_region_id
            , postal_code
            , case 
                when country_region_id = 'US'
                    then state_province_name
                else country_region_name
            end as distribution_center_name
            , spatial_location_address   
            , is_state_equal_country_flag
            , source_last_updated_at
    from location_joined
)

select * 
from transformed
