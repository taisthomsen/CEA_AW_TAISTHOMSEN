
with 
    source as (
        select * 
        from {{ source('erp', 'stateprovinces') }}
    )

    , renamed as (
        select
            -- Primary Key
            cast(stateprovinceid as string) as state_province_id
            -- Foreign Keys
            ,cast(countryregioncode as string) as country_region_id
            ,cast(territoryid as string) as territory_id
            -- State/Province Information
            ,cast(name as varchar) as state_province_name
            ,cast(stateprovincecode as varchar) as state_province_code
            ,cast(isonlystateprovinceflag as boolean) as is_state_equal_country_flag
            -- System columns
            ,cast(rowguid as string) as rowguid
            ,cast(modifieddate as date) as last_updated_at
        
    from source
)

select * 
from renamed
