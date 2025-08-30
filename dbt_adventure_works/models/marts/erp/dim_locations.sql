with 
    int_locations as (
        select * 
        from {{ ref('int_locations') }}
    )

    , generate_sk as (
        select
            address_id
            , city_name
            , state_province_name
            , country_region_name
            , state_province_id
            , country_region_id
            , postal_code
            , address_line_1
            , address_line_2
            , spatial_location_address
            , source_last_updated_at
            , current_timestamp() as updated_at
        from int_locations
    )

    select * 
    from generate_sk