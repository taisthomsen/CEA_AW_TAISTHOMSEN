
with 
    source as (
        select * 
        from {{ source('erp', 'addresses') }}
    )

    ,renamed as (
        select
            -- Primary Key
            cast(addressid as string) as address_id
            -- Foreign Keys
            ,cast(stateprovinceid as string) as state_province_id
            -- Address Information
            ,cast(addressline1 as varchar) as address_line_1
            ,cast(addressline2 as varchar) as address_line_2
            ,cast(city as varchar) as city_name
            ,cast(postalcode as varchar) as postal_code
            ,cast(spatiallocation as varchar) as spatial_location_address
            -- System columns
            ,cast(rowguid as string) as rowguid
            ,cast(modifieddate as date) as last_updated_at
            from source
        )

    select * 
    from renamed

