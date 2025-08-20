
with 
    source as (
        select * 
        from {{ source('erp', 'countryregions') }}
    )

    , renamed as (
        select
            -- Primary Key
            cast(countryregioncode as string) as country_region_id  
            -- Country/Region Information
            ,cast(name as varchar) as country_region_name
            -- System columns
            ,cast(modifieddate as date) as last_updated_at
        from source
    )

select * 
from renamed
