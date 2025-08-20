
with
    source as (
        select *
        from {{ source('erp', 'stores') }}
    )

    , renamed as (
        select
            -- Primary Key
            cast(businessentityid as string) as store_id
            -- Foreign Keys
            ,cast(salespersonid as string) as sales_person_id
            , name as store_name
            -- System columns
            ,cast(rowguid as string) as rowguid
            ,cast(modifieddate as date) as last_updated_at
            from source
        )

    select * 
    from renamed