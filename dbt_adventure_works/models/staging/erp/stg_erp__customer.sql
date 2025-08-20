
with
    source as (
        select *
        from {{ source('erp', 'customers') }}
    )

    , renamed as (
        select
            -- Primary Key
            cast(customerid as string) as customer_id
            -- Foreign Keys
            ,cast(personid as string) as person_id
            ,cast(storeid as string) as store_id
            ,cast(territoryid as string) as territory_id
            -- System columns
            ,cast(rowguid as string) as rowguid
            ,cast(modifieddate as date) as last_updated_at  
            from source
        )

    select * 
    from renamed