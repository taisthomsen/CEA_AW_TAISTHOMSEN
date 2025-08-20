
  with 
      source as (
        select * from {{ source('erp', 'salesreasons') }}
    )

    , renamed as (
        select
            -- Primary Key
            cast(salesreasonid as string) as sales_reason_id
            -- Sales Reason Information
            ,cast(name as varchar) as sales_reason_name
            ,cast(reasontype as varchar) as sales_reason_type
            -- System columns
            ,cast(modifieddate as date) as last_updated_at
        
    from source
)

select * 
from renamed
