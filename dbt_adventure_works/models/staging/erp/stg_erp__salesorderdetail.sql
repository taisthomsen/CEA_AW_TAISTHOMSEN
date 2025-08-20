
with 
    source as (
        select * 
        from {{ source('erp', 'salesorderdetails') }}
    )

    , renamed as (
        select
            -- Primary Key
            cast(salesorderdetailid as string) as sales_order_detail_id
            -- Foreign Keys
            ,cast(salesorderid as string) as sales_order_id
            ,cast(productid as string) as product_id
            ,cast(specialofferid as string) as special_offer_id
            -- Order Line Information
            ,cast(orderqty as integer) as order_qty
            ,cast(unitprice as numeric) as unit_price
            ,cast(unitpricediscount as numeric) as unit_price_discount
            -- System columns
            ,cast(rowguid as string) as rowguid
            ,cast(modifieddate as date) as last_updated_at
            from source
        )

    select * 
    from renamed
