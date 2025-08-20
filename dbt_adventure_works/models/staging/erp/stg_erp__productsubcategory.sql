
with 
    source as (
        select * from {{ source('erp', 'productsubcategories') }}
    )

    , renamed as (
        select
            -- Primary Key
            cast(productsubcategoryid as string) as product_subcategory_id
            -- Foreign Keys
            ,cast(productcategoryid as string) as product_category_id
            -- Product Subcategory Information
            ,cast(name as varchar) as product_subcategory_name
            -- System columns
            ,cast(rowguid as string) as rowguid
            ,cast(modifieddate as date) as last_updated_at
            from source
        )

    select * 
    from renamed    