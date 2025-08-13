{{
  config(
    materialized='view'
  )
}}

with source as (
    select * from {{ source('erp', 'salesorderdetail') }}
),

renamed as (
    select
        -- Primary Key
        salesorderdetailid as sales_order_detail_id,
        
        -- Foreign Keys
        salesorderid as sales_order_id,
        productid as product_id,
        specialofferid as special_offer_id,
        
        -- Order Line Information
        orderqty as order_qty,
        unitprice as unit_price,
        unitpricediscount as unit_price_discount,
        discount,
        line_total,
        rowguid,
        modifieddate as modified_date
        
    from source
)

select * from renamed
