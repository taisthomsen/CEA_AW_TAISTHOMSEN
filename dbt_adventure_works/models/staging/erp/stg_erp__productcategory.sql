
with
    source as (
        select * 
        from {{ source('erp', 'productcategories') }}
    )

    , renamed as (
        select
            -- Primary Key
            cast(productcategoryid as string) as product_category_id
            ,cast(name as varchar) as product_category_name
            -- System columns
            ,cast(rowguid as string) as rowguid
            ,cast(modifieddate as date) as last_updated_at
            from source
        )

    select * 
    from renamed